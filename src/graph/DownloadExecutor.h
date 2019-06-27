/* Copyright (c) 2019 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#ifndef GRAPH_DOWNLOADEXECUTOR_H
#define GRAPH_DOWNLOADEXECUTOR_H

#include "base/Base.h"
#include "graph/Executor.h"

namespace nebula {
namespace graph {

class DownloadExecutor final : public Executor {
public:
    DownloadExecutor(Sentence *sentence, ExecutionContext *ectx);

    const char* name() const override {
        return "DownloadExecutor";
    }

    Status MUST_USE_RESULT prepare() override;

    void execute() override;

private:
    DownloadExecutor                          *sentence_{nullptr};
};

}   // namespace graph
}   // namespace nebula

#endif  // GRAPH_DOWNLOADEXECUTOR_H
