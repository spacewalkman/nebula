/* Copyright (c) 2019 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#include "base/Base.h"
#include "graph/IngestExecutor.h"
#include "meta/SchemaManager.h"
#include "storage/client/StorageClient.h"
#include "dataman/RowWriter.h"

namespace nebula {
namespace graph {

IngestExecutor::IngestExecutor(Sentence *sentence,
                               ExecutionContext *ectx) : Executor(ectx) {
    sentence_ = static_cast<IngestSentence*>(sentence);
}

Status IngestExecutor::prepare() {
    return Status::OK();
}

void IngestExecutor::execute() {
    auto status = checkIfGraphSpaceChosen();
    if (!status.ok()) {
        DCHECK(onError_);
        onError_(std::move(status));
        return;
    }
    auto *mc = ectx()->getMetaClient();
    auto  addresses = mc->getAddresses();
    auto  metaHost = network::NetworkUtils::intToIPv4(addresses[0].first);
    auto  spaceId = ectx()->rctx()->session()->space();

    auto func = [metaHost, spaceId]() {
        static const char *tmp = "http://%s:%d/%s?space=%d";
        auto url = folly::stringPrintf(tmp, metaHost.c_str(),
                                       FLAGS_ws_meta_http_port,
                                       "ingest-dispatch", spaceId);
        auto result = http::HttpClient::get(url);
        if (result.ok() && result.value() == "SSTFile ingest successfully") {
            LOG(INFO) << "Ingest Successfully";
            return true;
        } else {
            LOG(ERROR) << "Ingest Failed";
            return false;
        }
    };
    auto future = folly::async(func);

    auto *runner = ectx()->rctx()->runner();

    auto cb = [this] (auto &&resp) {
        if (!resp) {
            DCHECK(onError_);
            onError_(Status::Error("Ingest Failed"));
            return;
        }
        resp_ = std::make_unique<cpp2::ExecutionResponse>();
        DCHECK(onFinish_);
        onFinish_();
    };

    auto error = [this] (auto &&e) {
        LOG(ERROR) << "Exception caught: " << e.what();
        DCHECK(onError_);
        onError_(Status::Error("Internal error"));
        return;

IngestExecutor::IngestExecutor(Sentence* sentence,
                               ExecutionContext* ectx)
    : Executor(ectx) {
    sentence_ = dynamic_cast<IngestSentence*>(sentence);
}

Status IngestExecutor::prepare() {
    Status status;
    do {
        auto spaceIdFromSession = ectx()->rctx()->session()->space();
        auto metaClient = ectx()->getMetaClient();
        auto spaceIdFromQuery =
            metaClient->getSpaceIdByNameFromCache(sentence_->getGraphSpaceName());
        if (spaceIdFromSession != spaceIdFromQuery) {
            // TODO: check permission
            FLOG_WARN("Ingest for graphspaceId %s is not the current session's graphspaceId %s",
                      spaceIdFromQuery, spaceIdFromSession);


        }

    } while (false);

    return status;
}

void IngestExecutor::execute() {
    auto future = ectx()->getMetaClient()->ingest(spaceIdFromQuery,
                                                    sentence_->getLocalDir(),
                                                    sentence_->force()).get();
    auto *runner = ectx()->rctx()->runner();

    auto cb = [this] (auto &&resp) {
      if (!resp.ok()) {
          DCHECK(onError_);
          onError_(std::move(resp).status());
          return;
      }
      auto  ret = std::move(resp).value();
      if (!ret) {
          DCHECK(onError_);
          onError_(Status::Error("Ingest sst files failed"));
          return;
      }
      DCHECK(onFinish_);
      onFinish_();
    };

    auto error = [this] (auto &&e) {
      LOG(ERROR) << "Exception caught: " << e.what();
      DCHECK(onError_);
      onError_(Status::Error("Internal error"));
      return;
    };

    std::move(future).via(runner).thenValue(cb).thenError(error);
}


void IngestExecutor::setupResponse(cpp2::ExecutionResponse &resp) {
    resp = std::move(*resp_);
}

}   // namespace graph
}   // namespace nebula
