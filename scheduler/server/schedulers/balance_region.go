// Copyright 2017 PingCAP, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// See the License for the specific language governing permissions and
// limitations under the License.

package schedulers

import (
	"fmt"
	"sort"

	"github.com/pingcap-incubator/tinykv/scheduler/server/core"
	"github.com/pingcap-incubator/tinykv/scheduler/server/schedule"
	"github.com/pingcap-incubator/tinykv/scheduler/server/schedule/operator"
	"github.com/pingcap-incubator/tinykv/scheduler/server/schedule/opt"
)

func init() {
	schedule.RegisterSliceDecoderBuilder("balance-region", func(args []string) schedule.ConfigDecoder {
		return func(v interface{}) error {
			return nil
		}
	})
	schedule.RegisterScheduler("balance-region", func(opController *schedule.OperatorController, storage *core.Storage, decoder schedule.ConfigDecoder) (schedule.Scheduler, error) {
		return newBalanceRegionScheduler(opController), nil
	})
}

const (
	// balanceRegionRetryLimit is the limit to retry schedule for selected store.
	balanceRegionRetryLimit = 10
	balanceRegionName       = "balance-region-scheduler"
)

type balanceRegionScheduler struct {
	*baseScheduler
	name         string
	opController *schedule.OperatorController
}

// newBalanceRegionScheduler creates a scheduler that tends to keep regions on
// each store balanced.
func newBalanceRegionScheduler(opController *schedule.OperatorController, opts ...BalanceRegionCreateOption) schedule.Scheduler {
	base := newBaseScheduler(opController)
	s := &balanceRegionScheduler{
		baseScheduler: base,
		opController:  opController,
	}
	for _, opt := range opts {
		opt(s)
	}
	return s
}

// BalanceRegionCreateOption is used to create a scheduler with an option.
type BalanceRegionCreateOption func(s *balanceRegionScheduler)

func (s *balanceRegionScheduler) GetName() string {
	if s.name != "" {
		return s.name
	}
	return balanceRegionName
}

func (s *balanceRegionScheduler) GetType() string {
	return "balance-region"
}

func (s *balanceRegionScheduler) IsScheduleAllowed(cluster opt.Cluster) bool {
	return s.opController.OperatorCount(operator.OpRegion) < cluster.GetRegionScheduleLimit()
}

func (s *balanceRegionScheduler) Schedule(cluster opt.Cluster) *operator.Operator {
	stores := make([]*core.StoreInfo, 0)
	for _, store := range cluster.GetStores() {
		if store.IsUp() && store.DownTime() < cluster.GetMaxStoreDownTime() {
			stores = append(stores, store)
		}
	}
	sort.Slice(stores, func(i, j int) bool {
		return stores[i].GetRegionSize() > stores[j].GetRegionSize()
	})

	for _, source := range stores {
		for retry := 0; retry < balanceRegionRetryLimit; retry++ {
			region := selectRegionForBalance(cluster, source.GetID())
			if region == nil {
				break
			}
			if len(region.GetStoreIds()) < cluster.GetMaxReplicas() {
				continue
			}

			var target *core.StoreInfo
			regionStores := region.GetStoreIds()
			for i := len(stores) - 1; i >= 0; i-- {
				candidate := stores[i]
				if candidate.GetRegionSize() >= source.GetRegionSize() {
					continue
				}
				if _, exists := regionStores[candidate.GetID()]; exists {
					continue
				}
				target = candidate
				break
			}
			if target == nil {
				continue
			}
			if source.GetRegionSize()-target.GetRegionSize() < 2*region.GetApproximateSize() {
				continue
			}

			newPeer, err := cluster.AllocPeer(target.GetID())
			if err != nil {
				return nil
			}
			op, err := operator.CreateMovePeerOperator(
				fmt.Sprintf(
					"balance region %d from store %d to store %d",
					region.GetID(),
					source.GetID(),
					target.GetID(),
				),
				cluster,
				region,
				operator.OpBalance,
				source.GetID(),
				target.GetID(),
				newPeer.GetId(),
			)
			if err == nil {
				return op
			}
		}
	}

	return nil
}

func selectRegionForBalance(cluster opt.Cluster, storeID uint64) *core.RegionInfo {
	var region *core.RegionInfo
	cluster.GetPendingRegionsWithLock(storeID, func(regions core.RegionsContainer) {
		region = regions.RandomRegion(nil, nil)
	})
	if region != nil {
		return region
	}
	cluster.GetFollowersWithLock(storeID, func(regions core.RegionsContainer) {
		region = regions.RandomRegion(nil, nil)
	})
	if region != nil {
		return region
	}
	cluster.GetLeadersWithLock(storeID, func(regions core.RegionsContainer) {
		region = regions.RandomRegion(nil, nil)
	})
	return region
}
