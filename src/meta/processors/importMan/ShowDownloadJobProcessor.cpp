/* Copyright (c) 2019 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#include "meta/processors/importMan/ShowDownloadJobProcessor.h"
#include "fs/HdfsUtils.h"

namespace nebula {
namespace meta {

void ShowDownloadJobProcessor::process(const cpp2::ShowImportJobReq &req) {
    // TODO: Query meta kvStore to retrieve status
}

}  // namespace meta
}  // namespace nebula
