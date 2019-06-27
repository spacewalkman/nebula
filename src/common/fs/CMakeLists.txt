file(GLOB FILE_SRCS LIST_DIRECTORIES false *.cpp)

message(STATUS "before remove HdfsUtils" ${FILE_SRCS})
message(STATUS "LIBHDFS_PATH = " ${LIBHDFS_PATH})

if (NOT ENABLE_HDFS_IMPORT_SUPPORT)
    list(REMOVE_ITEM ${FILE_SRCS} HdfsUtils.cpp)
    message(STATUS "after remove HdfsUtils" ${FILE_SRCS})
    add_library(fs_obj OBJECT ${FILE_SRCS})
else ()
    add_library(fs_obj OBJECT ${FILE_SRCS})
    target_link_libraries(fs_obj ${LIBHDFS_PATH} ${JAVA_JVM_LIBRARY})
endif (NOT ENABLE_HDFS_IMPORT_SUPPORT)

add_dependencies(fs_obj base_obj)

add_subdirectory(test)