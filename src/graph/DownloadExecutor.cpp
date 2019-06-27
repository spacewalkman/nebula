/* Copyright (c) 2018 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#include "base/Base.h"
#include "graph/ImportDataExecutor.h"
#include "meta/SchemaManager.h"
#include "storage/client/StorageClient.h"
#include "dataman/RowWriter.h"

namespace nebula {
namespace graph {

ImportDataExecutor::ImportDataExecutor(Sentence *sentence,
                                       ExecutionContext *ectx)
    : Executor(ectx) {
    sentence_ = dynamic_cast<ImportDataSentence *>(sentence);
}

Status ImportDataExecutor::prepare() {
    Status status;
    do {
        status = checkIfGraphSpaceChosen();
        if (!status.ok()) {
            break;
        }

        auto spaceId = ectx()->rctx()->session()->space();
        overwritable_ = sentence_->overwritable();
        schema_ = ectx()->getMetaClient()->
        if (schema_ == nullptr) {
            status = Status::Error("No schema found for '%s'",
                                   sentence_->edge()->c_str());
            break;
        }

        auto r = ectx()->getMetaClient()->getPartsAlloc(spaceId).get();
        if (!r.ok()) {
            return Status::Error(r.status().toString());
        }

    } while (false);

    return status;
}

void ImportDataExecutor::execute() {

}

}
}
