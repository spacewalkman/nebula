/* Copyright (c) 2018 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#ifndef GRAPH_IMPORTDATAEXECUTOR_H_
#define GRAPH_IMPORTDATAEXECUTOR_H_

#include "base/Base.h"
#include "graph/Executor.h"

namespace nebula {
namespace graph {

class DownloadExecutor final : public Executor {
public:
    DownloadExecutor(Sentence *sentence, ExecutionContext *ectx);

    const char* name() const override {
        return "ImportDataExecutor";
    }

    Status MUST_USE_RESULT prepare() override;

    void execute() override;

private:
    ImportDataExecutor                          *sentence_{nullptr};
};

}   // namespace graph
}   // namespace nebula


#endif  // GRAPH_IMPORTDATAEXECUTOR_H_

