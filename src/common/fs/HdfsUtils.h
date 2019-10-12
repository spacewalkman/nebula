/* Copyright (c) 2018 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#ifndef COMMON_FS_HDFSUTILS_H_
#define COMMON_FS_HDFSUTILS_H_

#include <folly/executors/IOThreadPoolExecutor.h>
#include <folly/futures/Future.h>
#include <gtest/gtest_prod.h>
#include "base/Base.h"
#include "base/StatusOr.h"
#include "cpp/helpers.h"
#include "fs/FileUtils.h"
#include "hdfs.h"

DECLARE_string(hdfs_namenode);
DECLARE_int32(namenode_port);
DECLARE_int32(download_bufferSize);
DECLARE_string(download_source_dir_pattern);
DECLARE_int32(download_thread_pool_size);

namespace nebula {
namespace fs {

class HdfsUtils final : public std::enable_shared_from_this<HdfsUtils>,
                        public cpp::NonCopyable,
                        public cpp::NonMovable {
    FRIEND_TEST(HdfsUtilsTest, ListRecursively);
    FRIEND_TEST(HdfsUtilsTest, ListFile);
    FRIEND_TEST(HdfsUtilsTest, StripLastFileComponent);
    FRIEND_TEST(HdfsUtilsTest, ParseHostAndPort);

public:
    static std::shared_ptr<HdfsUtils> getInstance(
        const std::string &url,
        std::function<bool(const hdfsFileInfo *fileInfo)> filter = defaultFilter);

    /**
     * Copy files asynchronously from hdfs dir at specific depth level, to local
     * dir
     *
     * @param hdfsDir hdfs source dir
     * @param localDir local destination dir
     * @param depth files in which depth level should be copied
     * @param downloadThreadPool thread pool to use
     * @param overwrite whether to override files already in localDir
     *
     * @return futures that hold all files copy status
     */
    std::vector<folly::Future<StatusOr<std::string>>> copyDir(
        folly::StringPiece hdfsDir,
        folly::StringPiece localDir,
        size_t depth,
        std::shared_ptr<folly::IOThreadPoolExecutor> downloadThreadPool,
        bool overwrite = false);

    bool copyFile(std::string &srcFile, std::string &dstFile);

    /**
     * list all sub dirs under parent dir, traverse one depth only
     *
     * @param hdfsDir parent dir
     * @param pattern sub dir name pattern to match
     * @return qualified sub dir names
     */
    std::unique_ptr<std::vector<std::string>> listSubDirs(const std::string &parentHdfsDir,
                                                          const std::string &pattern) const;

    /**
     * Count files in sub dir of the parent dir, traverse one depth only
     *
     * @param parentHdfsDir parent hdfs dir
     * @param pattern sub dir name pattern to match
     * @return map, key=sub dir name, value=file count
     */
    std::map<std::string, int> countFilesInSubDir(const std::string &parentHdfsDir,
                                                  const std::string &pattern);

    bool operator==(const HdfsUtils &rhs) const {
        return this->fs_ == rhs.fs_;
    }

    bool operator!=(const HdfsUtils &rhs) const {
        return !(this->operator==(rhs));
    }

private:
    static const std::function<bool(const hdfsFileInfo *fileInfo)> defaultFilter;

    std::unique_ptr<std::vector<std::string>> listFiles(folly::StringPiece hdfsDir) const;

    /**
     * list hdfs dir recursively, match every sub dir name to corresponding
     * patterns, collect matched files into results
     *
     * @param hdfsFileInfo hdfs dir
     * @param patterns sub dir name patterns to match
     * @param results matched files
     * @param depth current dir depth
     */
    void listRecursively(const hdfsFileInfo *hdfsFileInfo,
                         const std::vector<folly::StringPiece> &patterns,
                         std::vector<std::string> *results,
                         size_t depth) const;

    static std::string stripLastFileComponent(const std::string &path) {
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

    /**
     * ctor from a url like http://localhost:9000/ and a file filter
     */
    HdfsUtils(const std::string &url, std::function<bool(const hdfsFileInfo *fileInfo)> filter);

    static std::pair<std::string, tPort> parseHostAndPort(const std::string &url);

    // hdfsFS is actually dynamically allocated hdfs_internal*
    std::shared_ptr<hdfs_internal> fs_ = nullptr;

    // Which hdfs files qualify
    std::function<bool(const hdfsFileInfo *fileInfo)> filter_;
};

}   // namespace fs
}   // namespace nebula

#endif   // COMMON_FS_HDFSUTILS_H_
