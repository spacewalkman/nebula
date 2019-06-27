/* Copyright (c) 2018 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#ifndef STORAGE_ADDVERTICESPROCESSOR_H_
#define STORAGE_ADDVERTICESPROCESSOR_H_

#include "base/Base.h"
#include "storage/BaseProcessor.h"

namespace nebula {
namespace storage {

class AddVerticesProcessor : public BaseProcessor<cpp2::ExecResponse> {
public:
    static AddVerticesProcessor* instance(kvstore::KVStore* kvstore,
                                          meta::SchemaManager* schemaMan) {
        return new AddVerticesProcessor(kvstore, schemaMan);
    }

    void process(const cpp2::AddVerticesRequest& req);

private:
    AddVerticesProcessor(kvstore::KVStore* kvstore, meta::SchemaManager* schemaMan)
            : BaseProcessor<cpp2::ExecResponse>(kvstore, schemaMan) {}
};


}  // namespace storage
}  // namespace nebula
#endif  // STORAGE_ADDVERTICESPROCESSOR_H_
