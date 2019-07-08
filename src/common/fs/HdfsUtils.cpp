/* Copyright (c) 2018 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#include "fs/HdfsUtils.h"
#include <unistd.h>
#include <cstddef>
#include <algorithm>
#include <utility>
#include <vector>
#include "base/Base.h"
#include "base/CollectNSucceeded.h"
#include <folly/gen/Base.h>
#include <folly/executors/IOThreadPoolExecutor.h>

DEFINE_string(hdfs_namenode, "localhost", "Hdfs namenode's ip");
DEFINE_int32(namenode_port, 9000, "Hdfs namenode's port");
DEFINE_int32(download_bufferSize, 512, "Buffer size when downloading file from hdfs");
DEFINE_string(download_source_dir_pattern, ".+/\\d+/.+\\.sst$", "Hdfs source directory pattern");

namespace nebula {
namespace fs {

std::shared_ptr<HdfsUtils> HdfsUtils::getInstance
    (const char* namenode, tPort port,
     std::shared_ptr<folly::IOThreadPoolExecutor> downloadThreadPool) {
    static std::shared_ptr<HdfsUtils> instance(new HdfsUtils(namenode, port, downloadThreadPool));
    return instance;
}

std::vector<folly::Future<StatusOr<std::string>>> HdfsUtils::copyDir(folly::StringPiece hdfsDir,
                                                        folly::StringPiece localDir,
                                                        size_t depth,
                                                        bool overwrite) {
    CHECK(downloadThreadPool_);
    std::vector<folly::StringPiece> patterns;
    folly::split("/", FLAGS_download_source_dir_pattern, patterns, true);

    if (patterns.size() < depth) {
        return std::vector<folly::Future<StatusOr<std::string>>>();
    }

    if (overwrite) {
        FileUtils::remove(localDir.data(), true);
        FileUtils::makeDir(localDir.toString());
    }

    std::unique_ptr<std::vector<std::string>> files(listFiles(hdfsDir));
    if (files->empty()) {
        return std::vector<folly::Future<StatusOr<std::string>>>();
    }

    std::vector<std::pair<std::string, std::string>> filePairs;
    std::transform(files->begin(),
                   files->end(),
                   std::back_inserter(filePairs),
                   [=](const std::string& fileName) -> std::pair<std::string, std::string> {
                     std::vector<folly::StringPiece> components;
                     folly::split("/", fileName, components, true);

                     std::string lastDepthComponents;
                     for (size_t i = components.size() - depth; i < components.size(); i++) {
                         lastDepthComponents += "/" + components[i].toString();
                     }

                     return std::make_pair(fileName, localDir.toString() + lastDepthComponents);
                   });

    auto eb = downloadThreadPool_->getEventBase();
    auto futures = folly::gen::from(filePairs)
        | folly::gen::map([&](std::pair<std::string, std::string>& fpairs) {
          return folly::via(eb).then([self = shared_from_this(), &fpairs]() {
                    return self->copyFile(std::get<0>(fpairs), std::get<1>(fpairs));
                 }).then([](bool status) {
                     return Status::OK();
                 }).onError([&](std::exception& e) {
                     FLOG_ERROR("Copy file from %s to %s failed.",
                                std::get<0>(fpairs), std::get<1>(fpairs));
                     return Status::Error(std::get<0>(fpairs));
                 });

        })
        | folly::gen::as<std::vector>();

    std::vector<folly::Future<Status<std::string>>> ret(std::move(futures));
    return ret;

//
//        futures.wait();
//        CHECK(!futures.hasException())
//            << "Got exception when copy dir from hdfs to local: "
//            << futures.result().exception().what().toStdString();


}

// reference: https://github.com/apache/hadoop/tree/trunk/hadoop-hdfs-project/hadoop-hdfs-native-client/src/main/native/libhdfs
bool HdfsUtils::copyFile(std::string& srcFile, std::string& dstFile) {
    CHECK(fs_);
    hdfsFile
        src = ::hdfsOpenFile(fs_.get(), srcFile.data(), O_RDONLY, FLAGS_download_bufferSize, 0, 0);
    if (!src) {
        FLOG_ERROR("Failed to open source hdfs file: %s", srcFile.data());
        return false;
    }

    FVLOG4("copying %s ---> %s", srcFile.data(), dstFile.data());

    // make sure dest dir exists
    std::string destParentDir(stripLastFileComponent(dstFile));

    if (!FileUtils::makeDir(destParentDir)) {
        FLOG_ERROR("Failed to create dest local dir: %s", destParentDir.data());
    }

    FILE* destFp = ::fopen(dstFile.data(), "wb");
    if (!destFp) {
        FLOG_ERROR("Failed to open destination local file: %s", dstFile.data());
        return false;
    }

    void* buffer = malloc(sizeof(char) * FLAGS_download_bufferSize);
    if (!buffer) {
        FLOG_ERROR("Can't malloc buffer size=%d", FLAGS_download_bufferSize);
        return false;
    }

    ssize_t readSize = FLAGS_download_bufferSize;
    do {
        readSize = ::hdfsRead(fs_.get(), src, buffer, FLAGS_download_bufferSize);
        ::write(destFp->_fileno, buffer, readSize);
    } while (readSize == FLAGS_download_bufferSize);

    ::write(destFp->_fileno, buffer, readSize);

    ::free(buffer);
    ::hdfsCloseFile(fs_.get(), src);
    ::fclose(destFp);

    return true;
}

std::unique_ptr<std::vector<std::string>> HdfsUtils::listSubDirs(folly::StringPiece hdfsDir,
                                                                 const std::string& pattern) {
    auto trimmed = folly::trimWhitespace(hdfsDir);
    hdfsFileInfo* parentDir = hdfsGetPathInfo(fs_.get(), trimmed.data());
    int fileCount = -1;
    auto* subFiles = ::hdfsListDirectory(fs_.get(), parentDir->mName, &fileCount);

    auto ret = std::make_unique < std::vector < std::string >> ();
    if (fileCount > 0) {
        std::regex regex(pattern);
        for (int i = 0; i < fileCount; i++) {
            if (subFiles[i].mKind == kObjectKindDirectory
                && std::regex_match(subFiles[i].mName, regex)) {
                ret->emplace_back(subFiles[i].mName);
            }
        }

        hdfsFreeFileInfo(subFiles, fileCount);
    }

    return ret;
}

std::unique_ptr<std::vector<std::string>> HdfsUtils::listFiles(folly::StringPiece hdfsDir) {
    std::vector<folly::StringPiece> patterns;
    //TODO: handle directory separator other than UNIX
    folly::split("/", FLAGS_download_source_dir_pattern, patterns, true);

    auto trimmed = folly::trimWhitespace(hdfsDir);
    if (!trimmed.empty()) {
        hdfsFileInfo* parentDir = hdfsGetPathInfo(fs_.get(), trimmed.data());
        if (parentDir) {
            auto results = std::make_unique < std::vector < std::string >> ();
            listRecursively(parentDir, patterns, results.get(), 0);
            hdfsFreeFileInfo(parentDir, 1);
            parentDir = nullptr;

            return results;
        }
    }

    return nullptr;
}

void HdfsUtils::listRecursively(const hdfsFileInfo* hdfsFileInfo,
                                const std::vector<folly::StringPiece>& patterns,
                                std::vector<std::string>* results,
                                size_t depth) {
    if (depth == patterns.size()) {
        return;
    }

    // match per sub directory naming pattern
    std::regex currentPattern(patterns[depth]);
    auto currentName = FileUtils::basename(hdfsFileInfo->mName);

    if (!std::regex_match(currentName, currentPattern)) {
        return;
    }

    // only collect files reside in the deepest level
    if (depth == (patterns.size() - 1)) {
        if (hdfsFileInfo->mKind == kObjectKindFile) {
            results->push_back(hdfsFileInfo->mName);
        }
    }

    int fileCount = 0;
    auto* subFiles = ::hdfsListDirectory(fs_.get(), hdfsFileInfo->mName, &fileCount);
    if (fileCount == 0) {
        return;
    }

    size_t tempDepth = depth + 1;
    for (auto nextPtr = subFiles; nextPtr < subFiles + fileCount; nextPtr++) {
        listRecursively(nextPtr, patterns, results, tempDepth);
    }

    hdfsFreeFileInfo(subFiles, fileCount);
}

}
}