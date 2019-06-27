/* Copyright (c) 2018 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

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

};

}  // namespace meta
}  // namespace nebula

#endif //META_IMPORTDATAPROCESSOR_H_
