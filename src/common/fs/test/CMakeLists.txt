nebula_add_test(
    NAME file_utils_test
    SOURCES FileUtilsTest.cpp
    OBJECTS $<TARGET_OBJECTS:base_obj>
    LIBRARIES gtest gtest_main fs_obj
)

nebula_add_test(
    NAME temp_dir_test
    SOURCES TempDirTest.cpp
    OBJECTS $<TARGET_OBJECTS:base_obj>
    LIBRARIES gtest fs_obj
)

nebula_add_test(
    NAME temp_file_test
    SOURCES TempFileTest.cpp
    OBJECTS $<TARGET_OBJECTS:base_obj>
    LIBRARIES gtest gtest_main fs_obj
)

nebula_add_test(
    NAME hdfs_file_utils_test
    SOURCES HdfsUtilsTest.cpp
    OBJECTS $<TARGET_OBJECTS:base_obj>
    LIBRARIES gtest gtest_main fs_obj
)
