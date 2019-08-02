/* Copyright (c) 2018 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

<<<<<<< Updated upstream
#ifndef META_IMPORTDATAPROCESSOR_H_
#define META_IMPORTDATAPROCESSOR_H_
namespace nebula {
namespace meta {

class ImportDataProcessor : public BaseProcessor<cpp2::ImportDataReq> {
public:
    static ImportDataProcessor *instance(kvstore::KVStore *kvstore) {
        return new ImportDataProcessor(kvstore);
    }

    void process(const cpp2::ImportDataReq &req);

private:
    explicit ImportDataProcessor(kvstore::KVStore *kvstore)
        : BaseProcessor<cpp2::ImportDataReq>(kvstore) {}

    std::shared_ptr<StorageClient> storageClient_;

=======
#ifndef META_DOWNLOADPROCESSOR_H_
#define META_DOWNLOADPROCESSOR_H_

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
        : BaseProcessor<cpp2::LongRunnigTaskResp>(kvstore) {}

    static std::unique_ptr<common.PartitionID> toPartID(std::vector<std::string>* dirs,
                                                        common.PartitionID* maxPartID);
>>>>>>> Stashed changes
};

}  // namespace meta
}  // namespace nebula

<<<<<<< Updated upstream
#endif //META_IMPORTDATAPROCESSOR_H_
=======
#endif //META_DOWNLOADPROCESSOR_H_
>>>>>>> Stashed changes
