/* Copyright (c) 2018 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#ifndef COMMON_FS_HDFSUTILS_H_
#define COMMON_FS_HDFSUTILS_H_

#include "hdfs.h"
#include "base/StatusOr.h"
#include "fs/FileUtils.h"
#include "cpp/helpers.h"
#include "base/Base.h"
#include <gtest/gtest_prod.h>
#include <folly/futures/Future.h>
#include <folly/executors/IOThreadPoolExecutor.h>

DECLARE_string(hdfs_namenode);
DECLARE_int32(namenode_port);
DECLARE_int32(download_bufferSize);
DECLARE_string(download_source_dir_pattern);
DECLARE_int32(download_thread_pool_size);

namespace nebula {
namespace fs {

class HdfsUtils final
    : public std::enable_shared_from_this<HdfsUtils>
      , public cpp::NonCopyable
      , public cpp::NonMovable {
    FRIEND_TEST(HdfsUtilsTest, ListRecursivelyTest);
    FRIEND_TEST(HdfsUtilsTest, ListFilesTest);
    FRIEND_TEST(HdfsUtilsTest, StripLastFileComponentTest);

public:
    static std::shared_ptr<HdfsUtils> getInstance
        (const char* namenode, tPort port,
         std::shared_ptr<folly::IOThreadPoolExecutor> downloadThreadPool);

    std::vector<folly::Future<StatusOr<std::string>>> copyDir(folly::StringPiece hdfsDir,
                                                       folly::StringPiece localDir,
                                                       size_t depth,
                                                       bool overwrite = false);

    bool copyFile(std::string& srcFile, std::string& dstFile);

    std::unique_ptr<std::vector<std::string>> listSubDirs(folly::StringPiece hdfsDir,
                                                          const std::string& pattern);

    bool operator==(const HdfsUtils& rhs) const {
        return this->fs_ == rhs.fs_;
    }

    bool operator!=(const HdfsUtils& rhs) const {
        return !(*this == rhs);
    }

private:
    std::unique_ptr<std::vector<std::string>> listFiles(folly::StringPiece hdfsDir);

    void listRecursively(const hdfsFileInfo* hdfsFileInfo,
                         const std::vector<folly::StringPiece>& patterns,
                         std::vector<std::string>* results,
                         size_t depth);

    static std::string stripLastFileComponent(const std::string& path) {
        DCHECK(!path.empty() && path != "/");
        std::string ret(path);
        if (ret[ret.size() - 1] != '/') {
            auto lastSlash = ret.find_last_of("/");
            if (lastSlash != std::string::npos) {
                ret = ret.substr(0, lastSlash);
            }
        }

        return ret;
    }

    HdfsUtils(const char* namenode,
              tPort port,
              std::shared_ptr<folly::IOThreadPoolExecutor> downloadThreadPool) :
        downloadThreadPool_(downloadThreadPool) {
        fs_.reset(hdfsConnect(namenode, port), [](hdfsFS hdfs) {
          if (hdfsDisconnect(hdfs)) {
              LOG(WARNING) << "hdfsDisconnect failed";
          }
        });
    }

    // hdfsFS is actually dynamically allocated hdfs_internal*
    std::shared_ptr<hdfs_internal> fs_ = nullptr;

    //TODO: should be injeceted from Executor OR Processor
    std::shared_ptr<folly::IOThreadPoolExecutor> downloadThreadPool_;
};

}
}

#endif  // COMMON_FS_HDFSUTILS_H_
