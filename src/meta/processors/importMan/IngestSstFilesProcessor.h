/* Copyright (c) 2019 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#ifndef META_INGESTSSTFILESPROCESSOR_H_
#define META_INGESTSSTFILESPROCESSOR_H_

#include "meta/processors/BaseProcessor.h"

namespace nebula {
namespace meta {

class IngestSstFilesProcessor : public BaseProcessor<cpp2::LongRunningJobResp> {
public:
    static IngestSstFilesProcessor *instance(kvstore::KVStore *kvstore) {
        return new IngestSstFilesProcessor(kvstore);
    }

    void process(const ::nebula::cpp2::IngestSstFilesReq &req);

    static const std::string kJobIdKey;

private:
    explicit IngestSstFilesProcessor(kvstore::KVStore *kvstore)
        : BaseProcessor<cpp2::LongRunningJobResp>(kvstore) {}
};

}  // namespace meta
}  // namespace nebula

#endif  // META_INGESTSSTFILESPROCESSOR_H_
