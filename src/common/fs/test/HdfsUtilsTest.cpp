/* Copyright (c) 2018 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#include "base/Base.h"
#include <gtest/gtest.h>
#include "fs/HdfsUtils.h"
#include "fs/TempDir.h"
#include <thread>

namespace nebula {
namespace fs {

TEST(HdfsUtilsTest, SingletonTest) {
    auto downloadThreadPool= std::make_shared<folly::IOThreadPoolExecutor>(2);
    const auto& hdfsUtils1 = HdfsUtils::getInstance("localhost", 9000, downloadThreadPool);
    const auto& hdfsUtils2 = HdfsUtils::getInstance("localhost", 9000, downloadThreadPool);

    ASSERT_EQ(std::addressof(*(hdfsUtils1.get())), std::addressof(*(hdfsUtils2.get())));
}

TEST(HdfsUtilsTest, ListRecursivelyTest) {
    auto downloadThreadPool= std::make_shared<folly::IOThreadPoolExecutor>(2);
    const auto& hdfsUtils = HdfsUtils::getInstance("localhost", 9000, downloadThreadPool);

    //TODO: set up hdfs env to contain the required directory structure
    auto results = hdfsUtils->listFiles("/listRecursivelyTest");
    ASSERT_EQ(2, results->size());
    std::vector<std::string> expected {
        "hdfs://localhost:9000/listRecursivelyTest/1/vertex-12345.sst",
        "hdfs://localhost:9000/listRecursivelyTest/2/edge-23456.sst" };
    EXPECT_EQ(expected, *results);

    // change the pattern, so we can go one depth further
    FLAGS_download_source_dir_pattern = ".+/.+/\\d+/.+\\.sst$";

    auto moreDepthResults = hdfsUtils->listFiles("/listRecursivelyTest-parent1");
    ASSERT_EQ(2, results->size());
    std::vector<std::string> moreDepthExpected {
        "hdfs://localhost:9000/listRecursivelyTest-parent1/listRecursivelyTest/1/vertex-12345.sst",
        "hdfs://localhost:9000/listRecursivelyTest-parent1/listRecursivelyTest/2/edge-23456.sst" };
    ASSERT_EQ(moreDepthExpected, *moreDepthResults);
}

TEST(HdfsUtilsTest, CopyFileTest) {
    std::string srcFile{"hdfs://localhost:9000/listRecursivelyTest/1/vertex-12345.sst"};
    fs::TempDir localDir("/tmp/HdfsUtilsTest-CopyFileTest.XXXXXX");
    std::string localFile{localDir.path()};
    localFile += "/1";

    if (::access(localFile.c_str(), F_OK) < 0) {
        // FLOG_INFO("creating parent dir `%s`", localFile.c_str());
        if (::mkdir(localFile.c_str(), S_IRWXU | S_IRWXG | S_IRWXO ) == -1) {
            FLOG_ERROR("Failed to create dest local dir: `%s`", localFile.c_str());
        }

        CHECK(::access(localFile.c_str(), F_OK) == 0);
    }

    localFile +="/vertex-12345.sst";

    auto downloadThreadPool= std::make_shared<folly::IOThreadPoolExecutor>(2);
    const auto& hdfsUtils = HdfsUtils::getInstance("localhost", 9000, downloadThreadPool);
    auto ret = hdfsUtils->copyFile(srcFile, localFile);
    ASSERT_TRUE(ret);
}

TEST(HdfsUtilsTest, CopyDirTest) {
    auto downloadThreadPool= std::make_shared<folly::IOThreadPoolExecutor>(2);
    const auto& hdfsUtils = HdfsUtils::getInstance("localhost", 9000, downloadThreadPool);

    {
        std::string hdfsDir{"hdfs://localhost:9000/listRecursivelyTest/"};
        fs::TempDir localDir("/tmp/HdfsUtilsTest-CopyDirTest.XXXXXX");

        auto futures = hdfsUtils->copyDir(hdfsDir.c_str(), localDir.path(), 2);
        for (auto& f : futures) {
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
        auto futures = hdfsUtils->copyDir(hdfsDir.c_str(), localDir.path(), 3);
        for (auto& f : futures) {
            f.wait();
            ASSERT_TRUE(f.value().status().ok());
        }
    }
}

TEST(HdfsUtilsTest, StripLastFileComponentTest) {
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

TEST(HdfsUtilsTest, ListSubDirsTest) {
    std::string parentDir{"hdfs://localhost:9000/listRecursivelyTest/"};
    auto downloadThreadPool= std::make_shared<folly::IOThreadPoolExecutor>(2);
    const auto& hdfsUtils = HdfsUtils::getInstance("localhost", 9000, downloadThreadPool);
    auto ret1 = hdfsUtils->listSubDirs(parentDir, "\\d+");
    ASSERT_NE(ret1, nullptr);
    ASSERT_EQ(std::vector<std::string>{"hdfs://localhost:9000/listRecursivelyTest/1",
                                       "hdfs://localhost:9000/listRecursivelyTest/2"}, *ret1);
}

}
}

int main(int argc, char** argv) {
    testing::InitGoogleTest(&argc, argv);
    folly::init(&argc, &argv, true);
    google::SetStderrLogging(google::INFO);

    return RUN_ALL_TESTS();
}