/* Copyright (c) 2018 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#include "fs/HdfsUtils.h"
#include "meta/processors/importMan/DownloadProcessor.h"

namespace nebula {
namespace meta {

void DownloadProcessor::process(const cpp2::DownloadReq& req) {
    // TODO: If we lock the graphspace, it will hold all other space-level modification OP,
    // There is a dilemma here: It is the desired behavior, but it is also a long-running task
    // It will hold other task for a long time.

    //  To prevent running multiple DOWNLOAD in parallel,
    //  which would saturate the cpu and io resources
    folly::SharedMutex::WriteHolder wHolder(LockUtils::spaceLock());

    // This is much the same as GetPartsAllocProcessor's logic
    folly::SharedMutex::ReadHolder rHolder(LockUtils::spaceLock());
    auto spaceId = req.get_space_id();
    auto prefix = MetaServiceUtils::partPrefix(spaceId);
    std::unique_ptr<kvstore::KVIterator> iter;
    auto ret = kvstore_->prefix(kDefaultSpaceId, kDefaultPartId, prefix, &iter);
    if (ret != kvstore::ResultCode::SUCCEEDED) {
        onFinished();
        return;
    }

    // TODO: Should check that all part is allocated to a HostAddr, there should be no hole
    std::unordered_map<PartitionID, std::vector<HostAddr >> parts;
    std::set<PartitionID> partIdSetInMeta;
    PartitionID maxPartIdInMeta = -1;
    while (iter->valid()) {
        auto key = iter->key();
        PartitionID partId;
        memcpy(&partId, key.data() + prefix.size(), sizeof(PartitionID));
        std::vector<cpp2::HostAddr> partHosts = MetaServiceUtils::parsePartVal(iter->val());
        parts.emplace(partId, std::move(partHosts));
        partIdSetInMeta.emplace(partId);

        if (maxPartIdInMeta < partId) {
            maxPartIdInMeta = partId;
        }

        iter->next();
    }

    // TODO: this is based on the convention that PartitionID is a Zero-based,
    //  IF otherwise, need change the test condition here
    if (maxPartIdInMeta != parts.size() - 1) {
        resp_.set_code(cpp2::ErrorCode::E_HOLE_IN_PART_ALLOCATION);
        onFinished();
        return;
    }

    // check if there is any inconsistence between hdfs source dir structure and
    // There should be no partitionId not know by meta server
    auto subDirsPtr = HdfsUtils::listSubDir(req.get_source_dir(), "\\d+");
    PartitionID maxPartIdInHdfs = -1;
    auto partIdsSetInHdfs = toPartID(subDirsPtr.get(), maxPartIdInHdfs);

    std::set<PartitionID> diff;
    std::set_difference(partIdsSetInHdfs.begin(),
                        partIdsSetInHdfs.end(),
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

    // Now all clear, Inform all <PartitionID, vector<HostAddr>> to download in parallel
    kvstore_->asyncDownload(req.get_space_id(),
                            req.get_source_dir(),
                            req.get_dest_dir(),
                            req.get_force(),
                            //TODO:这里应该使用关联的kvstore_实例来更新任务进度
                            [this](cpp2::LongRunnigTaskResp& resp) {
                              this->resp_ = resp;
                                //TODO:记录task信息
                              this->onFinished();
                            });

    // TODO:记录task信息
//    std::vector<kvstore::KV> data;
//    for (auto& h : req.get_hosts()) {
//        data.emplace_back(MetaServiceUtils::hostKey(h.ip, h.port),
//                          MetaServiceUtils::hostValOffline());
//    }
//
//    doPut(std::move(data));

    //TODO: onStart=record execution plan to every single sst files; onFinished  = remove that every single entry

//    resp_.set_task_id();

}

std::unique_ptr<std::set<PartitionID>> DownloadProcessor::toPartID(std::vector<std::string>* dirs,
                                                                   PartitionID& maxPartID) {

    CHECK(dirs);
    auto ret = std::make_unique < std::set < PartitionID >> ();
    std::for_each(dirs->begin(), dirs->end(), [](std::string& dir) {
      try {
          auto tempPartID = folly::to<PartitionID>(dir);
          if (tempPartID < 0) {
              FLOG_ERROR("Illegal dir name %i, must be positive integer.", tempPartID);
          } else {
              ret.emplace(tempPartID);
              if (maxPartID < tempPartID) {
                  maxPartID = tempPartID;
              }
          }
      } catch (const std::runtime_error& e) {
          FLOG_ERROR("Sub dir name %s not a PartitionID.", e.what());
      }
    });

    if (ret.size() != dirs.size()) {
        FLOG_ERROR("Duplicate partitionID in hdfs dir.");
    }

    return ret;

}

}  // namespace meta
}  // namespace nebula
