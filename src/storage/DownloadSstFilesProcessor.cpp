/* Copyright (c) 2018 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#include "storage/DownloadSstFilesProcessor.h"
#include "dataman/RowReader.h"
#include "dataman/RowWriter.h"
#include "fs/HdfsUtils.h"
#include "time/Duration.h"
#include <algorithm>

namespace nebula {
namespace storage {

void DownloadSstFilesProcessor::process(const cpp2::DownloadReq &req) {
  VLOG(3) << "Receive DownloadReq...";

  const auto &hdfsUtils = HdfsUtils::getInstance(
      FLAGS_hdfs_namenode, FLAGS_namenode_port, getThreadManager());

  std::vector<cpp2::PartitionID> partitionToDownloads(
      std::move(req.get_partIds()));
  if (partitionToDownloads.empty()) {
    auto &partSubDirs = hdfsUtils->listSubDirs(req.get_source_dir(), "^\\d+$");
    std::move(partSubDirs.begin(), partSubDirs.end(),
              std::back_inserter(partitionToDownloads));
  }

  std::vector<folly::future<StatusOr<std::string>>> allTasks;
  for (auto partId : partitionToDownloads) {
    auto sourceDir = folly::fprintf("%s/%s", req.get_source_dir(), partId);
    auto destDir = folly::fprintf("%s/%s", req.get_dest_dir(), partId);

    VLOG(3) << "Begin downloading " << sourceDir << "-->" << destDir;
    auto tasksForOnePart = hdfsUtils->copyDir(sourceDir, destDir, 1, force);

    std::move(tasksForOnePart.begin(), tasksForOnePart.end(),
              std::back_inserter(allTasks));
  }

  // TODO：getThreadManager()从哪里来？
  auto allResultFutures =
      folly::collectAll(allTasks)
          .via(getThreadManager())
          .then([this](std::vector<StatusOr<std::string>> results) {
            std::for_each(
                results.begin(), results.end(), [](StatusOr<std::string> &st) {
                  if (!st.status().ok()) {
                    this->pushResultCode(
                        cpp2::ErrorCode::E_DOWNLOAD,
                        extractPartIdFromFileName(result.value().c_str()));
                  }
                });

            /**
             * struct ResponseCommon {
            // Only contains the partition that returns error
            1: required list<ResultCode> failed_codes,
            // Query latency from storage service
            2: required i32 latency_in_us,
        }

             */

            this->onFinished();
          });

  resp_.
}

cpp2::PartitionID
DownloadSstFilesProcessor::extractPartIdFromFileName(folly::StringPiece fileName) {
  std::vector<folly::StringPiece> patterns;
  folly::split("/", fileName, patterns, true);

  CHECK_GE(patterns.size(), 2);

  folly::to<cpp2::PartitionID>(patterns[patterns.size() - 2]);
}

} // namespace storage
} // namespace nebula
