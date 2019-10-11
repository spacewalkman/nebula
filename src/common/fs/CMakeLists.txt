file(GLOB FILE_SRCS LIST_DIRECTORIES false *.cpp)

nebula_add_subdirectory(test)

message(STATUS "before remove HdfsUtils" ${FILE_SRCS})
message(STATUS "LIBHDFS_PATH = " ${LIBHDFS_PATH})

if (NOT ENABLE_HDFS_IMPORT_SUPPORT)
    list(REMOVE_ITEM ${FILE_SRCS} HdfsUtils.cpp)
    nebula_add_library(fs_obj OBJECT ${FILE_SRCS})
else ()
    nebula_add_library(fs_obj OBJECT ${FILE_SRCS})
    set(hdfs_libs ${LIBHDFS_PATH} ${JAVA_JVM_LIBRARY})
    set_target_properties(fs_obj PROPERTIES OBJECT_DEPENDS ${hdfs_libs})
endif (NOT ENABLE_HDFS_IMPORT_SUPPORT)

add_dependencies(fs_obj base_obj)