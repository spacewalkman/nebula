/* Copyright (c) 2018 - present, VE Software Inc. All rights reserved
 *
 * This source code is licensed under Apache 2.0 License
 *  (found in the LICENSE.Apache file in the root directory)
 */

#ifndef COMMON_BASE_BASE_H_
#define COMMON_BASE_BASE_H_

#include <sys/types.h>
#include <sys/stat.h>
#include <unistd.h>
#include <fcntl.h>

#include <thread>
#include <atomic>
#include <mutex>
#include <condition_variable>
#include <regex>
#include <chrono>
#include <limits>

#include <functional>
#include <string>
#include <memory>
#include <sstream>
#include <iostream>
#include <fstream>

#include <vector>
#include <map>
#include <set>
#include <list>
#include <unordered_map>
#include <unordered_set>
#include <queue>
#include <deque>
#include <tuple>

#include <cstdio>
#include <cstdlib>
#include <cstdint>
#include <cerrno>
#include <cstring>
#include <ctime>
#include <cassert>

#include <gflags/gflags.h>
#include <glog/logging.h>

#include <boost/variant.hpp>

#include <folly/init/Init.h>
#include <folly/String.h>
#include <folly/Range.h>
#include <folly/Hash.h>
#include <folly/Random.h>
#include <folly/Conv.h>
#include <folly/ThreadLocal.h>
#include <folly/Varint.h>
#include <folly/dynamic.h>
#include <folly/json.h>
#include <folly/RWSpinLock.h>

#include "thread/NamedThread.h"
//#include "base/StringUnorderedMap.h"

#define VE_MUST_USE_RESULT              __attribute__((warn_unused_result))
#define VE_DONT_OPTIMIZE                __attribute__((optimize("O0")))

#define VE_ALWAYS_INLINE                __attribute__((always_inline))
#define VE_ALWAYS_NO_INLINE             __attribute__((noinline))

#define VE_BEGIN_NO_OPTIMIZATION        _Pragma("GCC push_options") \
                                        _Pragma("GCC optimize(\"O0\")")
#define VE_END_NO_OPTIMIZATION          _Pragma("GCC pop_options")

#ifndef UNUSED
#define UNUSED(x) (void)(x)
#endif  // UNUSED

#ifndef COMPILER_BARRIER
#define COMPILER_BARRIER()              asm volatile ("":::"memory")
#endif  // COMPILER_BARRIER

#include "base/ThriftTypes.h"
// Formated logging
#define FLOG_FATAL(...) LOG(FATAL) << folly::stringPrintf(__VA_ARGS__)
#define FLOG_ERROR(...) LOG(ERROR) << folly::stringPrintf(__VA_ARGS__)
#define FLOG_WARN(...) LOG(WARNING) << folly::stringPrintf(__VA_ARGS__)
#define FLOG_INFO(...) LOG(INFO) << folly::stringPrintf(__VA_ARGS__)
#define FVLOG1(...) VLOG(1) << folly::stringPrintf(__VA_ARGS__)
#define FVLOG2(...) VLOG(2) << folly::stringPrintf(__VA_ARGS__)
#define FVLOG3(...) VLOG(3) << folly::stringPrintf(__VA_ARGS__)
#define FVLOG4(...) VLOG(4) << folly::stringPrintf(__VA_ARGS__)


namespace nebula {

// Types using in a graph
// Partition ID is defined as PartitionID in Raftex
using VertexID = int64_t;
using TagID = int32_t;
using TagVersion = int64_t;
using EdgeType = int32_t;
using EdgeRanking = int64_t;
using EdgeVersion = int64_t;

template<typename Key, typename T>
using UnorderedMap = typename std::conditional<
    std::is_same<Key, std::string>::value,
//    StringUnorderedMap<T>,
    std::unordered_map<std::string, T>,
    std::unordered_map<Key, T>
>::type;

// Useful type traits

// Tell if `T' is copy-constructible
template <typename T>
static constexpr auto is_copy_constructible_v = std::is_copy_constructible<T>::value;

// Tell if `T' is move-constructible
template <typename T>
static constexpr auto is_move_constructible_v = std::is_move_constructible<T>::value;

// Tell if `T' is copy or move constructible
template <typename T>
static constexpr auto is_copy_or_move_constructible_v = is_copy_constructible_v<T> ||
                                                        is_move_constructible_v<T>;

// Tell if `T' is constructible from `Args'
template <typename T, typename...Args>
static constexpr auto is_constructible_v = std::is_constructible<T, Args...>::value;

// Tell if `U' could be convertible to `T'
template <typename U, typename T>
static constexpr auto is_convertible_v = std::is_constructible<U, T>::value;

}  // namespace nebula
#endif  // COMMON_BASE_BASE_H_