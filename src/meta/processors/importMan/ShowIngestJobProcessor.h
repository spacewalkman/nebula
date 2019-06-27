/* Copyright (c) 2019 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#ifndef META_SHOWINGESTJOBPROCESSOR_H_
#define META_SHOWINGESTJOBPROCESSOR_H_

#include "meta/processors/BaseProcessor.h"

namespace nebula {
namespace meta {

class ShowIngestJobProcessor : public BaseProcessor<cpp2::ShowIngestJobResp> {
public:
    static ShowIngestJobProcessor *instance(kvstore::KVStore *kvstore) {
        return new ShowIngestJobProcessor(kvstore);
    }

    void process(const cpp2::ShowImportJobReq &req);

private:
    explicit ShowIngestJobProcessor(kvstore::KVStore *kvstore)
        : BaseProcessor<cpp2::ShowIngestJobResp>(kvstore) {}
};

}  // namespace meta
}  // namespace nebula

#endif  // META_SHOWINGESTJOBPROCESSOR_H_
