/* Copyright (c) 2019 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#include "meta/processors/importMan/DownloadSstFilesProcessor.h"
#include "base/CollectNSucceeded.h"
#include "fs/HdfsUtils.h"
#include "time/WallClock.h"

namespace nebula {
namespace meta {

const std::string DownloadSstFilesProcessor::kJobIdKey = "__job_id_download__";

const std::string DownloadSstFilesProcessor::kJobType = "job_type";
const std::string DownloadSstFilesProcessor::kHdfsDir = "hdfs_dir";
const std::string DownloadSstFilesProcessor::kLocalDir = "local_dir";
const std::string DownloadSstFilesProcessor::kStartTime = "start_time";
const std::string DownloadSstFilesProcessor::kEndTime = "end_time";
const std::string DownloadSstFilesProcessor::kTotalCount = "total_count";
const std::string DownloadSstFilesProcessor::kSuccessCount = "success_count";
const std::string DownloadSstFilesProcessor::kErrorCount = "error_count";
const std::string DownloadSstFilesProcessor::kJobStatus = "job_status";

void DownloadSstFilesProcessor::process(const ::nebula::cpp2::DownloadSstFilesReq &req) {
    // TODO: If we lock the graph space, it will hold all other space-level
    // modification OP, There is a dilemma here: It is the desired behavior, but
    // it is also a long-running task It will hold other task for a long time.
    folly::SharedMutex::WriteHolder wHolder(LockUtils::spaceLock());

    // This is much the same as GetPartsAllocProcessor's logic
    auto spaceId = req.get_space_id();
    auto prefix = MetaServiceUtils::partPrefix(spaceId);
    std::unique_ptr<kvstore::KVIterator> iter;
    auto ret = kvstore_->prefix(kDefaultSpaceId, kDefaultPartId, prefix, &iter);
    if (ret != kvstore::ResultCode::SUCCEEDED) {
        onFinished();
        return;
    }

    // Check that all part is allocated to a HostAddr, there should be no hole
    // in between, otherwise, sst files corresponding to the un-allocated
    // partition will not be downloaded
    std::map<nebula::cpp2::HostAddr, std::set<PartitionID>> hostPartsMap;
    std::set<PartitionID> partIdSetInMeta;
    PartitionID maxPartIdInMeta = -1;
    while (iter->valid()) {
        auto key = iter->key();
        PartitionID partId;
        memcpy(&partId, key.data() + prefix.size(), sizeof(PartitionID));

        std::vector<nebula::cpp2::HostAddr> partHosts = MetaServiceUtils::parsePartVal(iter->val());
        for (auto &partHost : partHosts) {
            auto &partsSet = hostPartsMap[partHost];
            partsSet.emplace(partId);
        }

        partIdSetInMeta.emplace(partId);

        if (maxPartIdInMeta < partId) {
            maxPartIdInMeta = partId;
        }

        iter->next();
    }

    // TODO: this is based on the convention that PartitionID is a Zero-based,
    //  IF otherwise, need change the test condition here
    if (folly::to<size_t>(maxPartIdInMeta) != (partIdSetInMeta.size() - 1)) {
        resp_.set_code(cpp2::ErrorCode::E_HOLE_IN_PART_ALLOCATION);
        onFinished();
        return;
    }

    // Check if there is any inconsistence between hdfs source dir structure and
    // There should be no partitionId not know by meta server
    auto hdfsUtils = nebula::fs::HdfsUtils::getInstance(req.get_hdfs_dir());
    auto subDirsPtr = hdfsUtils->listSubDirs(req.get_hdfs_dir(), "^\\d+$");
    PartitionID maxPartIdInHdfs = -1;
    auto partIdsSetInHdfs = toPartID(subDirsPtr.get(), maxPartIdInHdfs);

    std::set<PartitionID> diff;
    std::set_difference(partIdsSetInHdfs->begin(),
                        partIdsSetInHdfs->end(),
                        partIdSetInMeta.begin(),
                        partIdSetInMeta.end(),
                        std::inserter(diff, diff.begin()));
    if (!diff.empty()) {
        LOG(ERROR) << "Some partition id not seen by meta server:";
        for (auto p : diff) {
            LOG(ERROR) << p;
        }

        resp_.set_code(cpp2::ErrorCode::E_INCONSIST_PART_BETWEEN_HDFS_AND_META);
        onFinished();
        return;
    }

    // Prevent running multiple DOWNLOAD in parallel, which could saturate cpu and network
    std::string val;
    nebula::cpp2::JobID jobId(folly::to<std::string>(req.get_space_id()));
    auto jobIdRet = kvstore_->get(kDefaultSpaceId, kDefaultPartId, jobId + "_" + kJobStatus, &val);
    if (jobIdRet == kvstore::ResultCode::SUCCEEDED) {
        auto jobStatus = folly::to<nebula::cpp2::JobStatus>(val.data());
        if (jobStatus == nebula::cpp2::JobStatus::INITIALIZING ||
            jobStatus == nebula::cpp2::JobStatus::RUNNING) {
            LOG(ERROR) << "There is another download job in progress";
            resp_.set_code(cpp2::ErrorCode::E_CONCURRENT_DOWNLOAD);
            onFinished();
            return;
        }
    } else if (jobIdRet == kvstore::ResultCode::ERR_KEY_NOT_FOUND) {   // No job start yet
        resp_.set_code(cpp2::ErrorCode::SUCCEEDED);
    } else {
        LOG(ERROR) << "Unexpected exception happens when downloading sst files, error_code="
                   << folly::to<std::string>(jobIdRet).c_str();
        resp_.set_code(cpp2::ErrorCode::E_KVSTORE);
        onFinished();
        return;
    }

    auto *evb = ioThreadPool_->getEventBase();
    auto successCallback = [jobId, evb, req, hostPartsMap, this](kvstore::ResultCode code) {
        if (code == kvstore::ResultCode::SUCCEEDED) {
            resp_.set_job_id(jobId);
            resp_.set_code(cpp2::ErrorCode::SUCCEEDED);

            // Now all clear, launch download job for each host asynchronously
            std::vector<folly::Future<storage::cpp2::ImportFilesResp>> storageDownloadFutures;
            storageDownloadFutures.resize(hostPartsMap.size());

            // Fan-out to all storage engine to download simultaneously
            // TODO: use groupByHost Flow control at Host(IP) level, not HostAddr level,
            // to prevent network saturation
            std::transform(
                hostPartsMap.begin(),
                hostPartsMap.end(),
                std::back_inserter(storageDownloadFutures),
                [evb, jobId, req, this](const auto &pair) {
                    auto storageClient = storageClientMan_->client(pair.first, evb);
                    // Wrap with jobId, then fanout
                    storage::cpp2::StorageDownloadSstFileReq storageDownloadRequest(
                        jobId, std::move(pair.second), req);
                    return storageClient->future_downloadSstFiles(storageDownloadRequest);
                });

            auto exceptedSize = storageDownloadFutures.size();
            auto updateWholeJobStatusCallback =
                [exceptedSize, jobId, this](
                    folly::Try<std::vector<storage::cpp2::ImportFilesResp>> &&result) {
                    if (result.hasException()) {
                        async_setJobStatus(jobId, ::nebula::cpp2::JobStatus::ERROR);
                    } else {
                        async_setJobStatus(jobId, ::nebula::cpp2::JobStatus::SUCCESS);
                    }
                };

            // Fan-in,when all succeed, update job status
            collectNSucceeded(storageDownloadFutures.begin(),
                              storageDownloadFutures.end(),
                              exceptedSize,
                              [](size_t, storage::cpp2::ImportFilesResp &resp) {
                                  return resp.get_code() ==
                                         nebula::storage::cpp2::ErrorCode::SUCCEEDED;
                              })
                .then(std::move(updateWholeJobStatusCallback));

        } else {   // TODO: should be more specific about other error
            resp_.set_code(cpp2::ErrorCode::E_KVSTORE);
        }

        onFinished();
    };

    auto errorCallback = [this](auto &&e) {
        LOG(ERROR) << "Exception caught: " << e.what();
        onFinished();
    };

    // Wait until all jobStatus persist
    auto jobInsertFuture =
        kvstore_->asyncMultiPut(kDefaultSpaceId, kDefaultPartId, populateJobStatus(req, jobId))
            .thenValue(successCallback)
            .thenError(errorCallback);
}

void DownloadSstFilesProcessor::async_setJobStatus(::nebula::cpp2::JobID jobId,
                                                   const ::nebula::cpp2::JobStatus &status) {
    kvstore_->asyncPut(kDefaultSpaceId,
                       kDefaultPartId,
                       {kJobStatus, reinterpret_cast<char *>(&status)},
                       [jobId, status](kvstore::ResultCode code) {
                           if (code != kvstore::ResultCode::SUCCEEDED) {
                               // TODO: What else can be done if this happens?
                               LOG(ERROR) << "Job " << jobId << " succeed, but set job status to "
                                          << status << " failed";
                           }
                       });
}

std::vector<nebula::kvstore::KV> DownloadSstFilesProcessor::populateJobStatus(
    const nebula::cpp2::DownloadSstFilesReq &req,
    const nebula::cpp2::JobID &jobId) {
    auto startTime = time::WallClock::fastNowInMilliSec();
    auto startTimeValue = reinterpret_cast<char *>(&startTime);

    auto hdfsUtils = nebula::fs::HdfsUtils::getInstance(req.get_hdfs_dir());
    const auto &subDirFileCountMap = hdfsUtils->countFilesInSubDir(req.get_hdfs_dir(), "^\\d+$");

    std::vector<nebula::kvstore::KV> jobStatusMap;

    // Since there must be only one download job for a single graph space, so
    // there is no need to distinguish between different download jobs, we use
    // graph space id as value, which is fixed.
    jobStatusMap.emplace_back(kJobIdKey, jobId.data());

    // Prepend jobId to all other status keys
    auto jobType = nebula::cpp2::ImportJobType::DOWNLOAD;
    jobStatusMap.emplace_back(jobId + "_" + kJobType, reinterpret_cast<char *>(&jobType));
    jobStatusMap.emplace_back(jobId + "_" + kHdfsDir, req.get_hdfs_dir().data());
    jobStatusMap.emplace_back(jobId + "_" + kStartTime, startTimeValue);
    jobStatusMap.emplace_back(jobId + "_" + kLocalDir, req.get_local_dir().data());
    jobStatusMap.emplace_back(jobId + "_" + kTotalCount, sum(subDirFileCountMap));
    // TODO: something bad happens here
    int zero = 0;
    jobStatusMap.emplace_back(jobId + "_" + kSuccessCount, reinterpret_cast<char *>(&zero));
    jobStatusMap.emplace_back(jobId + "_" + kErrorCount, reinterpret_cast<char *>(&zero));

    auto status = nebula::cpp2::JobStatus::INITIALIZING;
    jobStatusMap.emplace_back(jobId + "_" + kJobStatus, reinterpret_cast<char *>(&status));

    return jobStatusMap;
}

int DownloadSstFilesProcessor::sum(const std::map<std::string, int> &subDirFileCountMap) {
    int total = 0;
    for (const auto &pair : subDirFileCountMap) {
        total += pair.second;
    }

    return total;
}

std::unique_ptr<std::set<PartitionID>> DownloadSstFilesProcessor::toPartID(
    std::vector<std::string> *dirs,
    PartitionID &maxPartID) {
    CHECK(dirs);
    auto ret = std::make_unique<std::set<PartitionID>>();
    std::for_each(dirs->begin(), dirs->end(), [&maxPartID, retPtr = ret.get()](std::string &dir) {
        try {
            auto tempPartID = folly::to<PartitionID>(dir);
            if (tempPartID < 0) {
                FLOG_ERROR("Illegal dir name `%i`, must be positive integer.", tempPartID);
            } else {
                retPtr->emplace(tempPartID);
                if (maxPartID < tempPartID) {
                    maxPartID = tempPartID;
                }
            }
        } catch (const std::runtime_error &e) {
            FLOG_ERROR("Sub dir `%s` is not a PartitionID.", e.what());
        }
    });

    return ret;
}

}   // namespace meta
}   // namespace nebula
