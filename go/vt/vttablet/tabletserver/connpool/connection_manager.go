/*
Copyright 2022 The Vitess Authors.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

package connpool

import (
	"context"
	"fmt"
	"strings"
	"sync"
	"time"

	"vitess.io/vitess/go/pools"
)

type ConnManager struct {
	mu sync.Mutex

	settingsPool *pools.SettingsPool

	globalPool *pools.ResourcePool
}

func (cm *ConnManager) Open(globalPool *pools.ResourcePool, settingsPool *pools.SettingsPool) {
	cm.globalPool = globalPool
	cm.settingsPool = settingsPool
}

func (cm *ConnManager) GetPools() (*pools.ResourcePool, *pools.SettingsPool) {
	return cm.GetGlobalPool(), cm.GetSettingsPool()
}

func (cm *ConnManager) GetGlobalPool() (globalPool *pools.ResourcePool) {
	cm.mu.Lock()
	defer cm.mu.Unlock()
	globalPool = cm.globalPool
	return globalPool
}

func (cm *ConnManager) GetSettingsPool() (settingsPool *pools.SettingsPool) {
	cm.mu.Lock()
	defer cm.mu.Unlock()
	settingsPool = cm.settingsPool
	return settingsPool
}

func (cm *ConnManager) Close() {
	globalPool, settingsPool := cm.GetPools()
	if globalPool != nil {
		if settingsPool != nil {
			cm.settingsPool.TransferResourcesAndClose(cm.globalPool)
		}
		cm.globalPool.Close()
	}

	cm.mu.Lock()
	defer cm.mu.Unlock()
	cm.settingsPool = nil
	cm.globalPool = nil
}

func (cm *ConnManager) Capacity() int64 {
	globalPool := cm.GetGlobalPool()
	if globalPool == nil {
		return 0
	}
	return globalPool.Capacity()
}

func (cm *ConnManager) InUse() int64 {
	globalPool, settingsPool := cm.GetPools()
	if globalPool == nil {
		return 0
	}
	if settingsPool == nil {
		return globalPool.InUse()
	}
	return globalPool.InUse() - settingsPool.Available()
}

func (cm *ConnManager) Available() int64 {
	globalPool, settingsPool := cm.GetPools()
	if globalPool == nil {
		return 0
	}
	if settingsPool == nil {
		return globalPool.Available()
	}
	return globalPool.Available() + settingsPool.Available()
}

func (cm *ConnManager) Active() int64 {
	globalPool := cm.GetGlobalPool()
	if globalPool == nil {
		return 0
	}
	return globalPool.Active()
}

func (cm *ConnManager) Put(conn *DBConn) {
	if conn != nil && conn.settings != "" {
		settingsPool := cm.GetSettingsPool()

		// We try to put the conn into the settingsPool, if the pool is closed, we close
		// the connection and put it back into the globalPool. We put it back to the globalPool
		// so it can be reused again later.
		if settingsPool != nil && cm.settingsPool.Put(conn, conn.settings) {
			return
		}
		conn.Close()
	}
	globalPool := cm.GetGlobalPool()
	if globalPool == nil {
		panic(ErrConnPoolClosed)
	}
	if conn == nil {
		cm.globalPool.Put(nil)
	} else {
		cm.globalPool.Put(conn)
	}
}

func (cm *ConnManager) Get(ctx context.Context, settings string) (dbConn *DBConn, err error) {
	globalPool := cm.GetGlobalPool()
	if globalPool == nil {
		return nil, ErrConnPoolClosed
	}
	if settings == "" {
		resource, err := globalPool.Get(ctx)
		if err != nil {
			return nil, err
		}
		return resource.(*DBConn), nil
	}
	settingsPool := cm.GetSettingsPool()
	if settingsPool == nil {
		return nil, ErrConnPoolClosed
	}
	resource := cm.settingsPool.GetIfExists(settings)
	if resource != nil {
		return resource.(*DBConn), nil
	}

	resource, err = globalPool.Get(ctx)
	if err != nil {
		return nil, err
	}
	dbConn = resource.(*DBConn)
	// set the setting on new conn
	err = dbConn.ApplySettings(settings)
	return
}

func (cm *ConnManager) SetCapacity(capacity int, updateCap func(cap int)) (err error) {
	// we want to update the capacity of the parent using updateCap and the globalPool's capacity
	// so we keep the lock for the whole lifetime of the function
	cm.mu.Lock()
	defer cm.mu.Unlock()
	globalPool := cm.globalPool
	if globalPool == nil {
		return ErrConnPoolClosed
	}
	err = globalPool.SetCapacity(capacity)
	if err != nil {
		return err
	}
	updateCap(capacity)
	return nil
}

func (cm *ConnManager) SetIdleTimeout(idleTimeout time.Duration) {
	globalPool := cm.GetGlobalPool()
	if globalPool == nil {
		return
	}
	globalPool.SetIdleTimeout(idleTimeout)
}

func (cm *ConnManager) StatsJSON(waiterQueueFull int64) string {
	globalPool := cm.GetGlobalPool()
	if globalPool == nil {
		return "{}"
	}
	res := globalPool.StatsJSON()
	closingBraceIndex := strings.LastIndex(res, "}")
	if closingBraceIndex == -1 { // unexpected...
		return res
	}
	return fmt.Sprintf(`%s, "WaiterQueueFull": %v}`, res[:closingBraceIndex], waiterQueueFull)
}

func (cm *ConnManager) MaxCap() int64 {
	globalPool := cm.GetGlobalPool()
	if globalPool == nil {
		return 0
	}
	return globalPool.MaxCap()
}

func (cm *ConnManager) WaitCount() int64 {
	globalPool := cm.GetGlobalPool()
	if globalPool == nil {
		return 0
	}
	return globalPool.WaitCount()
}

func (cm *ConnManager) WaitTime() time.Duration {
	globalPool := cm.GetGlobalPool()
	if globalPool == nil {
		return 0
	}
	return globalPool.WaitTime()
}

func (cm *ConnManager) IdleTimeout() time.Duration {
	globalPool := cm.GetGlobalPool()
	if globalPool == nil {
		return 0
	}
	return globalPool.IdleTimeout()
}

func (cm *ConnManager) IdleClosed() int64 {
	globalPool := cm.GetGlobalPool()
	if globalPool == nil {
		return 0
	}
	return globalPool.IdleClosed()
}

func (cm *ConnManager) Exhausted() int64 {
	globalPool := cm.GetGlobalPool()
	if globalPool == nil {
		return 0
	}
	return globalPool.Exhausted()
}
