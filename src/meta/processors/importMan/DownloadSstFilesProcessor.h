/* Copyright (c) 2019 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#ifndef META_DOWNLOADSSTFILESPROCESSOR_H_
#define META_DOWNLOADSSTFILESPROCESSOR_H_

#include "fs/HdfsUtils.h"
#include "gen-cpp2/StorageServiceAsyncClient.h"
#include "meta/processors/BaseProcessor.h"
#include "meta/processors/importMan/DownloadSstFilesProcessor.h"

namespace nebula {
namespace meta {

class DownloadSstFilesProcessor : public BaseProcessor<cpp2::LongRunningJobResp> {
public:
    static DownloadSstFilesProcessor *instance(kvstore::KVStore *kvstore) {
        return new DownloadSstFilesProcessor(kvstore);
    }

    void process(const ::nebula::cpp2::DownloadSstFilesReq &req);

    static const std::string kJobIdKey;
    static const std::string kJobType;
    static const std::string kHdfsDir;
    static const std::string kStartTime;
    static const std::string kEndTime;
    static const std::string kLocalDir;
    static const std::string kTotalCount;
    static const std::string kSuccessCount;
    static const std::string kErrorCount;
    static const std::string kJobStatus;

private:
    explicit DownloadSstFilesProcessor(kvstore::KVStore *kvstore)
        : BaseProcessor<cpp2::LongRunningJobResp>(kvstore) {}

    // Convert dir name to partitionId
    static std::unique_ptr<std::set<PartitionID>> toPartID(std::vector<std::string> *dirs,
                                                           PartitionID &maxPartID);

    //  std::map<cpp2::IPv4, std::set<PartitionID>>
    //  groupByHost(std::map<PartitionID, vector<HostAddr>> &partsAllocations);

    std::unique_ptr<folly::IOThreadPoolExecutor> ioThreadPool_{nullptr};
    std::unique_ptr<thrift::ThriftClientManager<storage::cpp2::StorageServiceAsyncClient>>
        storageClientMan_;

    void async_setJobStatus(nebula::cpp2::JobID jobId, const nebula::cpp2::JobStatus &status);

    std::vector<nebula::kvstore::KV> initJobStatus(const nebula::cpp2::DownloadSstFilesReq &req,
                                                   const nebula::cpp2::JobID &jobId);

    static int sum(const std::map<std::string, int> &subDirFileCountMap);
};

}   // namespace meta
}   // namespace nebula

#endif   // META_DOWNLOADSSTFILESPROCESSOR_H_
