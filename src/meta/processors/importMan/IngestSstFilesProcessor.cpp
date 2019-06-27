/* Copyright (c) 2019 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#include "meta/processors/importMan/IngestSstFilesProcessor.h"
#include "fs/HdfsUtils.h"

namespace nebula {
namespace meta {

const std::string IngestSstFilesProcessor::kJobIdKey = "__job_id_ingest__";

// Ingest all sst files from local dir
void IngestSstFilesProcessor::process(const ::nebula::cpp2::IngestSstFilesReq &req) {
    folly::SharedMutex::WriteHolder wHolder(LockUtils::spaceLock());
    auto spaceId = req.get_space_id();

    // Prevent concurrent ingest job
    std::string val;
    cpp2::JobID jobId = req.get_space_id();
    auto ret = kvstore_->get(kDefaultSpaceId, kDefaultPartId, jobId + "_" + kJobStatus, &val);
    if (ret == kvstore::ResultCode::SUCCEEDED) {
        jobId = *reinterpret_cast<const cpp2::JobStatus *>(val.data());
        if (jobStatus == cpp2::JobStatus::INITIALIZING || jobStatus == cpp2::JobStatus::RUNNING) {
            LOG(ERROR) << "There is another ingest job in progress";
            resp_.set_code(cpp2::ErrorCode::E_CONCURRENT_DOWNLOAD);
            onFinished();
            return;
        }
    } else if (ret == kvstore::ResultCode::ERR_KEY_NOT_FOUND) {
        resp_.set_code(cpp2::ErrorCode::SUCCEEDED);
    } else {
        LOG(ERROR) << "Unexpected situation occurs when ingesting sst files,"
                   << ret.value().c_str();
        resp_.set_code(cpp2::ErrorCode::E_IMPORT_UNKOWN);
        onFinished();
        return;
    }

    auto jobInsertCallback = [jobId](kvstore::ResultCode code) {
        if (code == kvstore::ResultCode::SUCCEEDED) {
            resp_.set_job_id(jobId);
            resp_.set_code(cpp2::ErrorCode::SUCCEEDED);
        } else {
            resp_.set_code(cpp2::ErrorCode::E_IMPORT_UNKOWN);
        }

        onFinished();
    };

    auto error = [this](auto &&e) {
        LOG(ERROR) << "Exception caught: " << e.what();
        DCHECK(onError_);
        onError_(Status::Error("Internal error"));
    };

    // JobID must be inserted first
    auto jobInsertFuture = kvstore_->asyncPut(kDefaultSpaceId, kDefaultPartId, kjobIdKey, jobId)
                               .thenValue(jobInsertCallback)
                               .thenError(error);
    jobInsertFuture.wait();

    // Now all clear, launch ingest job for each host asynchronously
    auto *evb = ioThreadPool_->getEventBase();
    std::vector<folly::SemiFuture<ImportFilesResp>> storageIngestFutures;
    storageIngestFutures.resize(hostParts.size());
    std::transform(
        hostPartsMap.begin(), hostPartsMap.end(), std::back_inserter(storageIngestFutures),
        [evb](const auto &pair) {
            auto storageClient = storageClientMan_->client(pair.first, evb);
            cpp2::storageIngestRequest storageIngestRequest{jobId, std::move(pair.second), req};
            storageClient->future_ingestSstFiles(request);
        });

    collectNSucceeded(storageIngestFutures.begin(), storageIngestFutures.end(),
                      storageIngestFutures.size(), [](size_t, cpp2::ImportFilesResp &resp) {
                          return resp.get_error_code() == cpp2::ErrorCode::SUCCEEDED;
                      });
}

std::vector<nebula::kvstore::KV> IngestSstFilesProcessor::populateJobStatus(
    const cpp2::DownloadSstFilesReq &req, const cpp2::JobID &jobId) {
    auto startTime = WallClock::fastNowInMilliSec();
    auto startTimeValue = reinterpret_cast<char *>(&startTime);

    auto &subDirFileCountMap = HdfsUtils::countFilesInSubDir(req.get_hdfs_dir(), "^\\d+$");

    const int zero = 0;
    std::vector<nebula::kvstore::KV> jobStatusMap;

    // Since there must be only one download job for a single graph space, so
    // there is no need to distinguish between different download jobs, we use
    // graph space id as value, which is fixed.
    jobStatusMap.emplace_back(kJobIdKey, jobId.data());

    // Prepend jobId to all other status keys
    jobStatusMap.emplace_back(jobId + "_" + kJobType,
                              reinterpret_cast<char *>(&cpp2::ImportJobType::INGEST));
    jobStatusMap.emplace_emplace_backback(jobId + "_" + kHdfsDir, req.get_hdfs_dir().data());
    jobStatusMap.emplace_back(jobId + "_" + kStartTime, startTimeValue);
    jobStatusMap.emplace_back(jobId + "_" + kLocalDir, req.get_local_dir().data());
    jobStatusMap.emplace_back(jobId + "_" + kTotalCount, sum(subDirFileCountMap));
    jobStatusMap.emplace_back(jobId + "_" + kSuccessCount, reinterpret_cast<char *>(&zero));
    jobStatusMap.emplace_back(jobId + "_" + kErrorCount, reinterpret_cast<char *>(&zero));
    jobStatusMap.emplace_back(jobId + "_" + kJobStatus,
                              reinterpret_cast<char *>(&cpp2::JobStatus::INITIALIZING));

    return jobStatusMap;
}

}  // namespace meta
}  // namespace nebula
