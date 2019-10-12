/* Copyright (c) 2019 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#include "fs/HdfsUtils.h"
#include <folly/executors/IOThreadPoolExecutor.h>
#include <folly/gen/Base.h>
#include <unistd.h>
#include <algorithm>
#include <cstddef>
#include <stdexcept>
#include <utility>
#include <vector>
#include "base/Base.h"
#include "base/CollectNSucceeded.h"

DEFINE_int32(download_bufferSize, 512, "Buffer size when downloading file from hdfs");
DEFINE_string(download_source_dir_pattern, ".+/\\d+/.+\\.sst$", "Hdfs source directory pattern");

namespace nebula {
namespace fs {

const std::function<bool(const hdfsFileInfo *fileInfo)> HdfsUtils::defaultFilter =
    [](const hdfsFileInfo *fileInfo) { return fileInfo->mSize > 0; };

HdfsUtils::HdfsUtils(const std::string &url,
                     std::function<bool(const hdfsFileInfo *fileInfo)> filter) {
    auto pair = parseHostAndPort(url);
    filter_ = std::move(filter);
    fs_.reset(hdfsConnect(pair.first.data(), pair.second), [](hdfsFS hdfs) {
        if (hdfsDisconnect(hdfs)) {
            LOG(WARNING) << "hdfsDisconnect failed";
        }
    });
}

std::pair<std::string, tPort> HdfsUtils::parseHostAndPort(const std::string &url) {
    folly::StringPiece temp(url.data());
    auto schemeIndex = temp.find("://");

    // Grammar check should be done at query engine level, this is just in case
    if (UNLIKELY(schemeIndex == folly::StringPiece::npos)) {
        LOG(FATAL) << "Illegal hdfs url format: " << url;
        throw std::domain_error("Illegal hdfs url format:" + url);
    }

    auto sub = temp.subpiece(schemeIndex + 3);
    auto host = sub.split_step(":");
    std::string hostStr(host.start(), host.end());
    auto port = sub.split_step("/", [&](folly::StringPiece portStr) {
        return folly::to<tPort>(std::atoi(portStr.data()));
    });

    return std::pair<std::string, tPort>(hostStr, port);
}

std::shared_ptr<HdfsUtils> HdfsUtils::getInstance(
    const std::string &url, std::function<bool(const hdfsFileInfo *fileInfo)> filter) {
    static std::shared_ptr<HdfsUtils> instance(new HdfsUtils(url, filter));
    return instance;
}

std::vector<folly::Future<StatusOr<std::string>>> HdfsUtils::copyDir(
    folly::StringPiece hdfsDir, folly::StringPiece localDir, size_t depth,
    std::shared_ptr<folly::IOThreadPoolExecutor> downloadThreadPool, bool overwrite) {
    CHECK(downloadThreadPool);
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
    std::transform(files->begin(), files->end(), std::back_inserter(filePairs),
                   [=](const std::string &fileName) -> std::pair<std::string, std::string> {
                       std::vector<folly::StringPiece> components;
                       folly::split("/", fileName, components, true);

                       std::string lastDepthComponents;
                       for (size_t i = components.size() - depth; i < components.size(); i++) {
                           lastDepthComponents += "/" + components[i].toString();
                       }

                       return std::make_pair(fileName, localDir.toString() + lastDepthComponents);
                   });

    auto eb = downloadThreadPool->getEventBase();
    return folly::gen::from(filePairs) |
           folly::gen::map([&](std::pair<std::string, std::string> &fpairs) {
               return folly::via(eb)
                   .then([self = shared_from_this(), &fpairs]() {
                       return self->copyFile(std::get<0>(fpairs), std::get<1>(fpairs));
                   })
                   .thenValue([&](bool status) {
                       return StatusOr<std::string>(status ? Status::OK()
                                                           : Status::Error(std::get<0>(fpairs)));
                   })
                   .thenError([&](folly::exception_wrapper &&e) {
                       FLOG_ERROR("Copy file from %s to %s failed, %s", std::get<0>(fpairs).c_str(),
                                  std::get<1>(fpairs).c_str(), e.what().data());
                       return StatusOr<std::string>(Status::Error(std::get<0>(fpairs)));
                   });
           }) |
           folly::gen::as<std::vector>();
}

// reference:
// https://github.com/apache/hadoop/tree/trunk/hadoop-hdfs-project/hadoop-hdfs-native-client/src/main/native/libhdfs
bool HdfsUtils::copyFile(std::string &srcFile, std::string &dstFile) {
    CHECK(fs_);
    hdfsFile src =
        ::hdfsOpenFile(fs_.get(), srcFile.c_str(), O_RDONLY, FLAGS_download_bufferSize, 0, 0);
    if (!src) {
        FLOG_ERROR("Failed to open source hdfs file: %s", srcFile.c_str());
        return false;
    }

    FVLOG4("copying %s ---> %s", srcFile.c_str(), dstFile.c_str());

    // make sure dest dir exists
    std::string destParentDir(stripLastFileComponent(dstFile));

    if (!FileUtils::makeDir(destParentDir)) {
        FLOG_ERROR("Failed to create dest local dir: %s", destParentDir.c_str());
    }

    FILE *destFp = ::fopen(dstFile.c_str(), "wb");
    if (!destFp) {
        FLOG_ERROR("Failed to open destination local file: %s", dstFile.c_str());
        return false;
    }

    void *buffer = malloc(sizeof(char) * FLAGS_download_bufferSize);
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

std::unique_ptr<std::vector<std::string>> HdfsUtils::listSubDirs(const std::string &parentHdfsDir,
                                                                 const std::string &pattern) const {
    folly::StringPiece hdfsDir(parentHdfsDir.data());
    auto trimmed = folly::trimWhitespace(hdfsDir);
    hdfsFileInfo *parentDir = hdfsGetPathInfo(fs_.get(), trimmed.data());
    int fileCount = -1;
    auto *subFiles = ::hdfsListDirectory(fs_.get(), parentDir->mName, &fileCount);

    auto rets = std::make_unique<std::vector<std::string>>();
    if (fileCount > 0) {
        std::regex regex(pattern);
        for (int i = 0; i < fileCount; i++) {
            if (subFiles[i].mKind == kObjectKindDirectory) {
                auto fileName = folly::StringPiece(subFiles[i].mName);
                auto dirName = FileUtils::basename(fileName.data());
                if (std::regex_match(dirName, regex)) {
                    rets->emplace_back(dirName);
                }
            }
        }

        hdfsFreeFileInfo(subFiles, fileCount);
    }

    return rets;
}

std::map<std::string, int> HdfsUtils::countFilesInSubDir(
    const std::string &parentHdfsDir, const std::string &pattern) {
    folly::StringPiece hdfsDir(parentHdfsDir.data());
    auto trimmed = folly::trimWhitespace(hdfsDir);
    hdfsFileInfo *parentDir = hdfsGetPathInfo(fs_.get(), trimmed.data());
    int fileCount = 0;
    auto *subDirFiles = ::hdfsListDirectory(fs_.get(), parentDir->mName, &fileCount);

    auto ret = std::map<std::string, int>>();
    if (fileCount > 0) {
        std::regex regex(pattern);
        for (int i = 0; i < fileCount; i++) {
            if (subDirFiles[i].mKind == kObjectKindDirectory) {
                auto fileName = folly::StringPiece(subDirFiles[i].mName);
                auto dirName = FileUtils::basename(fileName.data());
                if (std::regex_match(dirName, regex)) {
                    int subDirFileCount = 0;
                    auto *innerFiles =
                        ::hdfsListDirectory(fs_.get(), subDirFiles[i].mName, &subDirFileCount);

                    if (subDirFileCount > 0) {
                        for (int j = 0; j < subDirFileCount; j++) {
                            // Only apply filter to file
                            if (innerFiles[i].mKind == kObjectKindFile && filter_(innerFiles + i)) {
                                hdfsFreeFileInfo(innerFiles, subDirFileCount);
                            }
                        }
                    }

                    ret[dirName] = subDirFileCount;
                }
            }
        }

        hdfsFreeFileInfo(subDirFiles, fileCount);
    }

    return ret;
}

std::unique_ptr<std::vector<std::string>> HdfsUtils::listFiles(
    folly::StringPiece parentHdfsDir) const {
    std::vector<folly::StringPiece> patterns;
    // TODO: handle directory separator other than UNIX
    folly::split("/", FLAGS_download_source_dir_pattern, patterns, true);

    auto trimmed = folly::trimWhitespace(parentHdfsDir);
    if (!trimmed.empty()) {
        hdfsFileInfo *parentDir = hdfsGetPathInfo(fs_.get(), trimmed.data());
        if (parentDir) {
            auto rets = std::make_unique<std::vector<std::string>>();
            listRecursively(parentDir, patterns, rets.get(), 0);
            hdfsFreeFileInfo(parentDir, 1);
            parentDir = nullptr;

            return rets;
        }
    }

    return nullptr;
}

void HdfsUtils::listRecursively(const hdfsFileInfo *hdfsFileInfo,
                                const std::vector<folly::StringPiece> &patterns,
                                std::vector<std::string> *results, size_t depth) const {
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
        if (hdfsFileInfo->mKind == kObjectKindFile && filter_(hdfsFileInfo)) {
            results->push_back(hdfsFileInfo->mName);
        }
    }

    int fileCount = 0;
    auto *subFiles = ::hdfsListDirectory(fs_.get(), hdfsFileInfo->mName, &fileCount);
    if (fileCount == 0) {
        return;
    }

    size_t tempDepth = depth + 1;
    for (auto nextPtr = subFiles; nextPtr < subFiles + fileCount; nextPtr++) {
        listRecursively(nextPtr, patterns, results, tempDepth);
    }

    hdfsFreeFileInfo(subFiles, fileCount);
}

}  // namespace fs
}