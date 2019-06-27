/* Copyright (c) 2018 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#include "base/Base.h"
#include "graph/DownloadExecutor.h"
#include "meta/SchemaManager.h"
#include "storage/client/StorageClient.h"
#include "dataman/RowWriter.h"

namespace nebula {
namespace graph {

DownloadExecutor::DownloadExecutor(Sentence* sentence,
                                   ExecutionContext* ectx)
    : Executor(ectx) {
    sentence_ = dynamic_cast<DownloadSentence*>(sentence);
}

Status DownloadExecutor::prepare() {
    Status status;
    do {
        auto spaceName = ectx()->rctx()->session()->spaceName();
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

void DownloadExecutor::execute() {

}

}
}
