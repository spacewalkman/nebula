/* Copyright (c) 2018 - present, VE Software Inc. All rights reserved
 *
 * This source code is licensed under Apache 2.0 License
 *  (found in the LICENSE.Apache file in the root directory)
 */

#ifndef KVSTORE_PARTMANAGER_H_
#define KVSTORE_PARTMANAGER_H_

#include <gtest/gtest_prod.h>
#include "base/Base.h"

namespace nebula {
namespace kvstore {

using MachineID = uint32_t;

struct PartMeta {
    GraphSpaceID           spaceId_;
    PartitionID            partId_;
    std::vector<MachineID> peers_;
};

//graphSpace和与其对应的所有partition(以及partition相关的meta)之间的mapping
using PartsMap  = std::unordered_map<GraphSpaceID, std::unordered_map<PartitionID, PartMeta>>;
/**
 * This class manages all meta information one storage host needed.
 * */
 //partition manager
class PartManager {
public:
    /**
     *  Singleton instance will be returned
     * */
    static PartManager* instance();

    virtual ~PartManager() = default;

    /**
     * return PartsMap for machineId
     * */
     //返回一个机器的所有的partition信息
    virtual PartsMap parts(HostAddr hostAddr) = 0;

    /**
     * return PartMeta for <spaceId, partId>
     * */
     //给定一个graphdb,partitionId返回这个指定的
    virtual PartMeta partMeta(GraphSpaceID spaceId, PartitionID partId) = 0;

protected:
    PartManager() = default;
    static PartManager* instance_;
};

/**
: * Memory based PartManager, it is used in UTs now.
 * */
class MemPartManager final : public PartManager {
    FRIEND_TEST(KVStoreTest, SimpleTest);
public:
    MemPartManager() = default;

    ~MemPartManager() = default;

    PartsMap parts(HostAddr hostAddr) override;

    PartMeta partMeta(GraphSpaceID spaceId, PartitionID partId) override;

    PartsMap& partsMap() {
        return partsMap_;
    }

private:
    PartsMap partsMap_;
};

}  // namespace kvstore
}  // namespace nebula
#endif  // KVSTORE_PARTMANAGER_H_

