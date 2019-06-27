/* Copyright (c) 2018 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#include "storage/DownloadSstFileProcessor.h"
#include <algorithm>
#include "time/Duration.h"
#include "dataman/RowReader.h"
#include "dataman/RowWriter.h"

namespace nebula {
namespace storage {

void DownloadSstFileProcessor::process(const cpp2::DownloadReq &req) {
    spaceId_ = req.get_space_id();
    EdgeContext edgeContext;
    std::vector<TagContext> tagContexts;
    // By default, _src, _rank, _dst will be returned as the first 3 fields
    addDefaultProps(edgeContext);
    int32_t returnColumnsNum =
        req.get_return_columns().size() + edgeContext.props_.size();
    auto retCode = this->checkAndBuildContexts(req, tagContexts, edgeContext);
    if (retCode != cpp2::ErrorCode::SUCCEEDED) {
        for (auto &p : req.get_parts()) {
            this->pushResultCode(retCode, p.first);
        }
        this->onFinished();
        return;
    }

    RowSetWriter rsWriter;
    std::for_each(req.get_parts().begin(),
                  req.get_parts().end(),
                  [&](auto &partE) {
                    auto partId = partE.first;
                    kvstore::ResultCode ret;
                    for (auto &edgeKey : partE.second) {
                        ret = this->collectEdgesProps(partId,
                                                      edgeKey,
                                                      edgeContext.props_,
                                                      rsWriter);
                        if (ret != kvstore::ResultCode::SUCCEEDED) {
                            break;
                        }
                    }
                    // TODO handle failures
                    this->pushResultCode(this->to(ret), partId);
                  });
    resp_.set_data(std::move(rsWriter.data()));

    std::vector<PropContext> props;
    props.reserve(returnColumnsNum);
    for (auto &prop : edgeContext.props_) {
        props.emplace_back(std::move(prop));
    }
    std::sort(props.begin(), props.end(), [](auto &l, auto &r) {
      return l.retIndex_ < r.retIndex_;
    });
    decltype(resp_.schema) s;
    decltype(resp_.schema.columns) cols;
    for (auto &prop : props) {
        VLOG(3) << prop.prop_.name << ","
                << static_cast<int8_t>(prop.type_.type);
        cols.emplace_back(
            columnDef(std::move(prop.prop_.name),
                      prop.type_.type));
    }
    s.set_columns(std::move(cols));
    resp_.set_schema(std::move(s));
    this->onFinished();
}
}
}
