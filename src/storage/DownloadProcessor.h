/* Copyright (c) 2018 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#ifndef STORAGE_DOWNLOADPROCESSOR_H_
#define STORAGE_DOWNLOADPROCESSOR_H_

#include "base/Base.h"
#include "storage/DownloadProcessor.h"

namespace nebula {
namespace storage {

class DownloadProcessor : public BaseProcessor<cpp2::ExecResponse> {
public:
    static DownloadProcessor *instance(kvstore::KVStore *kvstore, meta::SchemaManager *schemaMan) {
      return new DownloadProcessor(kvstore);
    }

    void process(const cpp2::DownloadReq &req);

private:
    DownloadProcessor(kvstore::KVStore *kvstore, meta::SchemaManager *schemaMan, folly::Executor *executor)
        : BaseProcessor<cpp2::ExecResponse>(kvstore, schemaMan) {}

    static cpp2::PartitionID extractPartIdFromFileName(folly::StringPiece fileName);

    folly::Executor* executor_ = nullptr;
};

}
}

#endif //STORAGE_DOWNLOADPROCESSOR_H_