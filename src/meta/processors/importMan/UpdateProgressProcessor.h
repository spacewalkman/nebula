/* Copyright (c) 2019 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#ifndef META_UPDATEPROGRESSPROCESSOR_H_
#define META_UPDATEPROGRESSPROCESSOR_H_

#include "meta/processors/BaseProcessor.h"

namespace nebula {
namespace meta {

class UpdateProgressProcessor : public BaseProcessor<cpp2::UpdateProgressResp> {
public:
    static UpdateProgressProcessor *instance(kvstore::KVStore *kvstore) {
        return new UpdateProgressProcessor(kvstore);
    }

    void process(const cpp2::UpdateProgressReq &req);

private:
    explicit UpdateProgressProcessor(kvstore::KVStore *kvstore)
        : BaseProcessor<cpp2::UpdateProgressResp>(kvstore) {}
};

}  // namespace meta
}  // namespace nebula

#endif  // META_UPDATEPROGRESSPROCESSOR_H_
