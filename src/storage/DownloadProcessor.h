/* Copyright (c) 2018 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#ifndef STORAGE_DOWNLOADSSTFILEPROCESSOR_H_
#define STORAGE_DOWNLOADSSTFILEPROCESSOR_H_

#include "base/Base.h"
#include "storage/QueryBaseProcessor.h"

namespace nebula {
namespace storage {

class DownloadProcessor : public BaseProcessor<cpp2:ImportDataResponse> {
public:
    static DownloadSstFileProcessor* instance(kvstore::KVStore* kvstore, meta::SchemaManager* schemaMan) {
        return new DownloadSstFileProcessor(kvstore);
    }

    void process(const cpp2::DownloadSstFilesRequest& req);

private:
    explicit DownloadSstFileProcessor(kvstore::KVStore* kvstore, meta::SchemaManager* schemaMan)
    : BaseProcessor<cpp2::ImportDataResponse>(kvstore, schemaMan) {}
};

}
}

#endif //STORAGE_DOWNLOADSSTFILEPROCESSOR_H_
