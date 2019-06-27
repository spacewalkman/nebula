/* Copyright (c) 2019 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#include "meta/processors/importMan/KillJobProcessor.h"
#include "fs/HdfsUtils.h"

namespace nebula {
namespace meta {

void KillJobProcessor::process(const cpp2::KillJobReq &req) {
    // TODO: kill job's tasks and set job status to KILLED
}

}  // namespace meta
}  // namespace nebula
