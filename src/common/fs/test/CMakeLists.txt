nebula_add_test(
    NAME file_utils_test
    SOURCES FileUtilsTest.cpp
    OBJECTS $<TARGET_OBJECTS:fs_obj> $<TARGET_OBJECTS:base_obj>
    LIBRARIES gtest ${LIBHDFS_PATH} ${JAVA_JVM_LIBRARY}
)

nebula_add_test(
    NAME temp_dir_test
    SOURCES TempDirTest.cpp
    OBJECTS $<TARGET_OBJECTS:fs_obj> $<TARGET_OBJECTS:base_obj>
    LIBRARIES gtest ${LIBHDFS_PATH} ${JAVA_JVM_LIBRARY}
)

nebula_add_test(
    NAME temp_file_test
    SOURCES TempFileTest.cpp
    OBJECTS $<TARGET_OBJECTS:fs_obj> $<TARGET_OBJECTS:base_obj>
    LIBRARIES gtest ${LIBHDFS_PATH} ${JAVA_JVM_LIBRARY}
)

nebula_add_test(
        NAME hdfs_file_utils_test
        SOURCES HdfsUtilsTest.cpp
        OBJECTS $<TARGET_OBJECTS:fs_obj> $<TARGET_OBJECTS:base_obj>
        LIBRARIES gtest ${LIBHDFS_PATH} ${JAVA_JVM_LIBRARY}
)
