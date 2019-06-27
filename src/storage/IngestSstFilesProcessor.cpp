/* Copyright (c) 2018 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#include "storage/IngestSstFilesProcessor.h"
#include "dataman/RowReader.h"
#include "dataman/RowWriter.h"
#include "fs/HdfsUtils.h"
#include "time/Duration.h"
#include <algorithm>

namespace nebula {
namespace storage {

void IngestSstFilesProcessor::process(
    const cpp2::StorageIngestSstFileReq &req) {
  VLOG(3) << "Receive StorageIngestSstFileReq...";

  const auto &hdfsUtils = HdfsUtils::getInstance(
      FLAGS_hdfs_namenode, FLAGS_namenode_port, getThreadManager());

  std::vector<folly::future<StatusOr<std::string>>> allTasks;
  for (auto partId : req.get_partIds()) {
    auto sourceDir = folly::fprintf("%s/%s", req.get_source_dir(), partId);
    auto destDir = folly::fprintf("%s/%s", req.get_dest_dir(), partId);

    VLOG(3) << "Begin ingesting " << sourceDir << "-->" << destDir;
    auto tasksForOnePart = hdfsUtils->copyDir(sourceDir, destDir, 1, force);

    std::move(tasksForOnePart.begin(), tasksForOnePart.end(),
              std::back_inserter(allTasks));
  }

  // partial aggregation for a this hostAddr
  folly::collectAll(allTasks)
      .via(getThreadManager())
      .then([this](std::vector<StatusOr<std::string>> result) {
        std::for_each(
            result.begin(), result.end(), [](StatusOr<std::string> &st) {
              if (!st.status().ok()) {
                this->pushResultCode(
                    cpp2::ErrorCode::E_DOWNLOAD,
                    extractPartIdFromFileName(result.value().c_str()));
              }
            });

        this->onFinished();
      });
}

cpp2::PartitionID
DownloadProcessor::extractPartIdFromFileName(folly::StringPiece fileName) {
  std::vector<folly::StringPiece> patterns;
  folly::split("/", fileName, patterns, true);

  CHECK_GE(patterns.size(), 2);

  folly::to<cpp2::PartitionID>(patterns[patterns.size() - 2]);
}

} // namespace storage
} // namespace nebula
