/* Copyright (c) 2019 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#ifndef META_KILLJOBPROCESSOR_H_
#define META_KILLJOBPROCESSOR_H_

#include "meta/processors/BaseProcessor.h"

namespace nebula {
namespace meta {

class KillJobProcessor : public BaseProcessor<cpp2::KillJobResp> {
public:
    static KillJobProcessor *instance(kvstore::KVStore *kvstore) {
        return new KillJobProcessor(kvstore);
    }

    void process(const cpp2::KillJobReq &req);

private:
    explicit KillJobProcessor(kvstore::KVStore *kvstore)
        : BaseProcessor<cpp2::KillJobResp>(kvstore) {}
};

}  // namespace meta
}  // namespace nebula

#endif  // META_KILLJOBPROCESSOR_H_
