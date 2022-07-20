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

package pools

import (
	"sync"

	"vitess.io/vitess/go/sync2"
)

type SettingsPool struct {
	available sync2.AtomicInt64

	mu          sync.Mutex
	settingsMap map[string][]Resource
}

func NewSettingsPool() *SettingsPool {
	return &SettingsPool{
		settingsMap: map[string][]Resource{},
	}
}

func (sp *SettingsPool) TransferResourcesAndClose(globalPool *ResourcePool) {
	sp.mu.Lock()
	defer sp.mu.Unlock()

	for _, conns := range sp.settingsMap {
		for _, conn := range conns {
			globalPool.Put(conn)
			sp.available.Add(-1)
		}
	}
	sp.settingsMap = nil
}

func (sp *SettingsPool) IsClosed() (closed bool) {
	return sp.settingsMap == nil
}

func (sp *SettingsPool) Available() int64 {
	return sp.available.Get()
}

func (sp *SettingsPool) GetIfExists(settings string) Resource {
	sp.mu.Lock()
	defer sp.mu.Unlock()
	listOfCons := sp.settingsMap[settings]
	if len(listOfCons) == 0 {
		return nil
	}
	connToUse := listOfCons[0]
	sp.settingsMap[settings] = listOfCons[1:]
	return connToUse
}

func (sp *SettingsPool) Put(conn Resource, settings string) bool {
	sp.mu.Lock()
	defer sp.mu.Unlock()

	if sp.IsClosed() {
		return false
	}

	sp.settingsMap[settings] = append(sp.settingsMap[settings], conn)
	return true
}
