/* Copyright (c) 2018 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#include "meta/processors/importMan/DownloadProcessor.h"

namespace nebula {
namespace meta {

void DownloadProcessor::process(const cpp2::DownloadReq &req) {
    folly::SharedMutex::WriteHolder wHolder(LockUtils::spaceLock());

    //TODO: download sst files and do rocksdb ingestion in parallel
    std::vector<kvstore::KV> data;
    for (auto &h : req.get_hosts()) {
        data.emplace_back(MetaServiceUtils::hostKey(h.ip, h.port),
                          MetaServiceUtils::hostValOffline());
    }

    //TODO: 记录

    doPut(std::move(data));

    //TODO: onStart=record execution plan to every single sst files; onFinished  = remove that every single entry


}

}  // namespace meta
}  // namespace nebula
