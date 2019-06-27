/* Copyright (c) 2018 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#ifndef STORAGE_INGESTSSTFILESPROCESSOR_H_
#define STORAGE_INGESTSSTFILESPROCESSOR_H_

#include "base/Base.h"
#include "storage/IngestSstFilesProcessor.h"

namespace nebula {
namespace storage {

class IngestSstFilesProcessor : public BaseProcessor<cpp2::ImportFilesResp> {
public:
  static IngestSstFilesProcessor *instance(kvstore::KVStore *kvstore,
                                           meta::SchemaManager *schemaMan) {
    return new IngestSstFilesProcessor(kvstore);
  }

  void process(const cpp2::StorageIngestSstFileReq &req);

private:
  explicit IngestSstFilesProcessor(kvstore::KVStore *kvstore,
                                   meta::SchemaManager *schemaMan)
      : BaseProcessor<cpp2::ImportFilesResp>(kvstore, schemaMan) {}
};

} // namespace storage
} // namespace nebula

#endif // STORAGE_INGESTSSTFILESPROCESSOR_H_
