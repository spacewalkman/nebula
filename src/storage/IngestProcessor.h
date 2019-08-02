/* Copyright (c) 2018 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#ifndef STORAGE_INGESTPROCESSOR_H_
#define STORAGE_INGESTPROCESSOR_H_

#include "base/Base.h"
#include "storage/IngestProcessor.h"

namespace nebula {
namespace storage {

class IngestProcessor : public BaseProcessor<cpp2::ExecResponse> {
public:
    static IngestProcessor* instance(kvstore::KVStore* kvstore, meta::SchemaManager* schemaMan) {
        return new IngestProcessor(kvstore);
    }

    void process(const cpp2::IngestReq& req);

private:
    explicit IngestProcessor(kvstore::KVStore* kvstore, meta::SchemaManager* schemaMan)
    : BaseProcessor<cpp2::ExecResponse>(kvstore, schemaMan) {}

};

}
}

#endif //STORAGE_INGESTPROCESSOR_H_
