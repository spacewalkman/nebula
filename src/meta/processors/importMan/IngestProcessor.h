/* Copyright (c) 2018 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#ifndef META_INGESTPROCESSOR_H_
#define META_INGESTPROCESSOR_H_

#include "meta/processors/BaseProcessor.h"

namespace nebula {
namespace meta {

class DownloadProcessor : public BaseProcessor<cpp2::LongRunnigTaskResp> {
public:
    static DownloadProcessor *instance(kvstore::KVStore *kvstore) {
        return new DownloadProcessor(kvstore);
    }

    void process(const cpp2::DownloadReq &req);

private:
    explicit DownloadProcessor(kvstore::KVStore *kvstore)
        : BaseProcessor<cpp2::ImportDataReq>(kvstore) {}

    std::shared_ptr<StorageClient> storageClient_;

    static std::unique_ptr<common.PartitionID> toPartID(std::vector<std::string>* dirs,
                                                        common.PartitionID* maxPartID);

class IngestProcessor : public BaseProcessor<cpp2::LongRunnigTaskResp> {
public:
    static IngestProcessor *instance(kvstore::KVStore *kvstore) {
        return new IngestProcessor(kvstore);
    }

    void process(const cpp2::IngestReq &req);

private:
    explicit IngestProcessor(kvstore::KVStore *kvstore)
        : BaseProcessor<cpp2::LongRunnigTaskResp>(kvstore) {}
};

}  // namespace meta
}  // namespace nebula

#endif // META_INGESTPROCESSOR_H_
