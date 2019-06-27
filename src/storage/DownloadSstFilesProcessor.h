/* Copyright (c) 2019 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#ifndef STORAGE_DOWNLOADSSTFILESPROCESSOR_H_
#define STORAGE_DOWNLOADSSTFILESPROCESSOR_H_

#include "base/Base.h"
#include "storage/DownloadSstFilesProcessor.h"

namespace nebula {
namespace storage {

class DownloadSstFilesProcessor : public BaseProcessor<cpp2::ExecResponse> {
public:
  static DownloadSstFilesProcessor *instance(kvstore::KVStore *kvstore,
                                             meta::SchemaManager *schemaMan) {
    return new DownloadSstFilesProcessor(kvstore);
  }

  void process(const cpp2::DownloadReq &req);

private:
  DownloadSstFilesProcessor(kvstore::KVStore *kvstore,
                            meta::SchemaManager *schemaMan,
                            folly::Executor *executor)
      : BaseProcessor<cpp2::ExecResponse>(kvstore, schemaMan) {}

  static cpp2::PartitionID
  extractPartIdFromFileName(folly::StringPiece fileName);

  folly::Executor *executor_ = nullptr;
};

} // namespace storage
} // namespace nebula

#endif // STORAGE_DOWNLOADSSTFILESPROCESSOR_H_