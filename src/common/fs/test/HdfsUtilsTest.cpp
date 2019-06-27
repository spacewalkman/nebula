/* Copyright (c) 2018 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#include <gtest/gtest.h>
#include <thread>
#include "base/Base.h"
#include "fs/HdfsUtils.h"
#include "fs/TempDir.h"

namespace nebula {
namespace fs {

TEST(HdfsUtilsTest, ParseHostAndPort) {
    auto hostPortFromWithDir = HdfsUtils::parseHostAndPort("hdfs://localhost:9000/somedir");
    auto hostPortFromWithoutDir = HdfsUtils::parseHostAndPort("hdfs://localhost:9000/");

    std::pair<std::string, tPort> expected("localhost", 9000);
    ASSERT_EQ(hostPortFromWithDir, expected);
    ASSERT_EQ(hostPortFromWithoutDir, expected);
}

TEST(HdfsUtilsTest, Singleton) {
    const auto &hdfsUtils1 = HdfsUtils::getInstance("hdfs://localhost:9000/somedir");
    const auto &hdfsUtils2 = HdfsUtils::getInstance("hdfs://localhost:9000/");

    ASSERT_EQ(std::addressof(*(hdfsUtils1.get())), std::addressof(*(hdfsUtils2.get())));
}

TEST(HdfsUtilsTest, ListRecursively) {
    const auto &hdfsUtils = HdfsUtils::getInstance("hdfs://localhost:9000/");

    // TODO: set up hdfs env to contain the required directory structure
    auto results = hdfsUtils->listFiles("/listRecursivelyTest");
    ASSERT_EQ(2, results->size());
    std::vector<std::string> expected{
        "hdfs://localhost:9000/listRecursivelyTest/1/vertex-12345.sst",
        "hdfs://localhost:9000/listRecursivelyTest/2/edge-23456.sst"};
    EXPECT_EQ(expected, *results);

    // change the pattern, so we can go one depth further
    FLAGS_download_source_dir_pattern = ".+/.+/\\d+/.+\\.sst$";

    auto moreDepthResults = hdfsUtils->listFiles("/listRecursivelyTest-parent1");
    ASSERT_EQ(2, results->size());
    std::vector<std::string> moreDepthExpected{
        "hdfs://localhost:9000/listRecursivelyTest-parent1/listRecursivelyTest/1/"
        "vertex-12345.sst",
        "hdfs://localhost:9000/listRecursivelyTest-parent1/listRecursivelyTest/2/"
        "edge-23456.sst"};
    ASSERT_EQ(moreDepthExpected, *moreDepthResults);
}

TEST(HdfsUtilsTest, CopyFile) {
    std::string srcFile{"hdfs://localhost:9000/listRecursivelyTest/1/vertex-12345.sst"};
    fs::TempDir localDir("/tmp/HdfsUtilsTest-CopyFileTest.XXXXXX");
    std::string localFile{localDir.path()};
    localFile += "/1";

    if (::access(localFile.c_str(), F_OK) < 0) {
        // FLOG_INFO("creating parent dir `%s`", localFile.c_str());
        if (::mkdir(localFile.c_str(), S_IRWXU | S_IRWXG | S_IRWXO) == -1) {
            FLOG_ERROR("Failed to create dest local dir: `%s`", localFile.c_str());
        }

        CHECK(::access(localFile.c_str(), F_OK) == 0);
    }

    localFile += "/vertex-12345.sst";

    const auto &hdfsUtils = HdfsUtils::getInstance("hdfs://localhost:9000/");
    auto ret = hdfsUtils->copyFile(srcFile, localFile);
    ASSERT_TRUE(ret);
}

TEST(HdfsUtilsTest, CopyDir) {
    auto downloadThreadPool = std::make_shared<folly::IOThreadPoolExecutor>(2);
    const auto &hdfsUtils = HdfsUtils::getInstance("hdfs://localhost:9000/");

    {
        std::string hdfsDir{"hdfs://localhost:9000/listRecursivelyTest/"};
        fs::TempDir localDir("/tmp/HdfsUtilsTest-CopyDirTest.XXXXXX");

        std::vector<folly::Future<StatusOr<std::string>>> futures =
            hdfsUtils->copyDir(hdfsDir.c_str(), localDir.path(), 2, downloadThreadPool);
        for (auto &f : futures) {
            f.wait();
            ASSERT_TRUE(f.value().status().ok());
        }
    }

    // change the pattern, so we can go one depth further
    FLAGS_download_source_dir_pattern = ".+/.+/\\d+/.+\\.sst$";

    {
        std::string hdfsDir{"hdfs://localhost:9000/listRecursivelyTest-parent1/"};
        fs::TempDir localDir("/tmp/HdfsUtilsTest-CopyDirTest.XXXXXX");

        // FLOG_INFO("localDir= %s", localDir.path());
        std::vector<folly::Future<StatusOr<std::string>>> futures =
            hdfsUtils->copyDir(hdfsDir.c_str(), localDir.path(), 3, downloadThreadPool);
        for (auto &f : futures) {
            f.wait();
            ASSERT_TRUE(f.value().status().ok());
        }
    }
}

TEST(HdfsUtilsTest, StripLastFileComponent) {
    std::string withSuffix("/some/parent/dir/1/somefile.sst");
    auto ret1 = HdfsUtils::stripLastFileComponent(withSuffix);
    ASSERT_EQ("/some/parent/dir/1", ret1);

    std::string withNoSuffix("/some/parent/dir/1/somefile");
    auto ret2 = HdfsUtils::stripLastFileComponent(withNoSuffix);
    ASSERT_EQ("/some/parent/dir/1", ret2);

    std::string justDirs("/some/parent/dir/1/");
    auto ret3 = HdfsUtils::stripLastFileComponent(justDirs);
    ASSERT_EQ("/some/parent/dir/1/", ret3);
}

TEST(HdfsUtilsTest, ListSubDirs) {
    const auto &hdfsUtils = HdfsUtils::getInstance("hdfs://localhost:9000/");
    auto ret1 = hdfsUtils->listSubDirs("hdfs://localhost:9000/listRecursivelyTest/", "^\\d+$");
    std::vector<std::string> expected{"1", "2"};
    ASSERT_EQ(expected, *ret1);
}

TEST(HdfsUtilsTest, CountFilesInSubDir) {
    const auto &hdfsUtils = HdfsUtils::getInstance("hdfs://localhost:9000/");
    auto ret1 =
        hdfsUtils->countFilesInSubDir("hdfs://localhost:9000/listRecursivelyTest/", "^\\d+$");
    std::map<std::string, int> expected{{"1", 1}, {"2", 1}};
    ASSERT_EQ(expected, *ret1);
}

}  // namespace fs
}  // namespace nebula

int main(int argc, char **argv) {
    testing::InitGoogleTest(&argc, argv);
    folly::init(&argc, &argv, true);
    google::SetStderrLogging(google::INFO);

    return RUN_ALL_TESTS();
}