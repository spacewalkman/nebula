/* Copyright (c) 2019 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#include "base/StatusOr.h"
#include "http/HttpClient.h"
#include "graph/DownloadExecutor.h"
#include "process/ProcessUtils.h"
#include "webservice/Common.h"

#include <folly/executors/Async.h>
#include <folly/futures/Future.h>
#include <folly/executors/ThreadedExecutor.h>

namespace nebula {
namespace graph {

DownloadExecutor::DownloadExecutor(Sentence *sentence, ExecutionContext *ectx)
    : Executor(ectx) {
    sentence_ = dynamic_cast<DownloadExecutor *>(sentence);
}

//TODO: 让各个storage层调用其checkCapacity方法，看有没有足够的磁盘空间
Status DownloadExecutor::prepare() {
    return Status::OK();
}

void DownloadExecutor::execute() {
    //TODO: 向meta层转发该请求
}

}
}
