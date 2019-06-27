/* Copyright (c) 2018 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */


namespace cpp nebula
namespace java com.vesoft.nebula
namespace go nebula

cpp_include "base/ThriftTypes.h"

typedef i32 (cpp.type = "nebula::GraphSpaceID") GraphSpaceID
typedef i32 (cpp.type = "nebula::PartitionID") PartitionID
typedef i32 (cpp.type = "nebula::TagID") TagID
typedef i32 (cpp.type = "nebula::EdgeType") EdgeType
typedef i64 (cpp.type = "nebula::EdgeRanking") EdgeRanking
typedef i64 (cpp.type = "nebula::VertexID") VertexID

typedef i32 (cpp.type = "nebula::IPv4") IPv4
typedef i32 (cpp.type = "nebula::Port") Port

typedef i64 (cpp.type = "nebula::SchemaVer") SchemaVer

typedef i32 (cpp.type = "nebula::UserID") UserID
typedef i64 (cpp.type = "nebula::ClusterID") ClusterID

// Long-running job id
typedef string (cpp.type = "nebula::JobID") JobID

// These are all data types supported in the graph properties
enum SupportedType {
    UNKNOWN = 0,

    // Simple types
    BOOL = 1,
    INT = 2,
    VID = 3,
    FLOAT = 4,
    DOUBLE = 5,
    STRING = 6,

    // Date time
    TIMESTAMP = 21,
    YEAR = 22,
    YEARMONTH = 23,
    DATE = 24,
    DATETIME = 25,

    // Graph specific
    PATH = 41,

    // Container types
    // LIST = 101,
    // SET = 102,
    // MAP = 103,      // The key type is always a STRING
    // STRUCT = 104,
} (cpp.enum_strict)


struct ValueType {
    1: SupportedType type;
    // vtype only exists when the type is a LIST, SET, or MAP
    2: optional ValueType value_type (cpp.ref = true);
    // When the type is STRUCT, schema defines the struct
    3: optional Schema schema (cpp.ref = true);
} (cpp.virtual)

struct ColumnDef {
    1: required string name,
    2: required ValueType type,
}

struct SchemaProp {
    1: optional i64      ttl_duration,
    2: optional string   ttl_col,
}

struct Schema {
    1: list<ColumnDef> columns,
    2: SchemaProp schema_prop,
}

struct HostAddr {
    1: IPv4  ip,
    2: Port  port,
}

struct Pair {
    1: string key,
    2: string value,
}

const ValueType kInvalidValueType = {"type" : UNKNOWN}

// The following will be shared between meta and storage thrift
// All type of tasks
enum ImportJobType {
    DOWNLOAD  = 0x01,
    INGEST    = 0x02,
    //TODO: add other task type here, like BALANCE?
} (cpp.enum_strict)

// Download sst files request
struct DownloadSstFilesReq {
    // which graph space this download request for
    1: GraphSpaceID  space_id,
    // hdfs parent dir which hold all sst files for all partitions
    2: string        hdfs_dir,
    // some local dir that have sufficient disk capacity to hold sst files for all partitions it
    // holds, need to specified, because it need to be consistent among all host
    3: string        local_dir,
}

struct IngestSstFilesReq {
    // which graph space this download request for
    1: GraphSpaceID     space_id,
    // some local dir that sst files reside
    2: string           local_dir,
}

// What client get is async task id, import = download OR ingest
struct ImportSstFilesResp {
    // async download task id for query
    1: JobID       job_id,
}

// Long-running task status like Download or Ingest sst files, could be queried by `show task`
enum JobStatus {
    INITIALIZING    = 0x01,
    RUNNING         = 0x02,
    ERROR           = 0x03,
    SUCCESS         = 0x04,
    KILLED          = 0x05,
} (cpp.enum_strict)

// What storage server report to meta server after making progress
struct UpdateProgressReq {
    1: JobID        job_id,
    2: PartitionID  partition_id,
    // When normal, delta will be populated
    3: i64          delta,
    // When error, code will be populated
    4: JobStatus    status,
}