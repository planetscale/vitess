/*
Copyright 2023 The Vitess Authors.

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

package main

import (
	"context"
	_ "embed"
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"net/http"
	"os"
	"strings"
	"time"

	"golang.org/x/sync/errgroup"

	"vitess.io/vitess/go/mysql"
	"vitess.io/vitess/go/test/endtoend/cluster"
	"vitess.io/vitess/go/vt/log"
)

var (
	clusterInstance *cluster.LocalProcessCluster
	vtParams        mysql.ConnParams
	sKs             = "sks"
	cell            = "test"

	//go:embed sharded_schema.sql
	sSchemaSQL string

	//go:embed vschema.json
	sVSchema string
)

var (
	shards4 = []string{
		"-40", "40-80", "80-c0", "c0-",
	}

	shards8 = []string{
		"-20", "20-40", "40-60", "60-80", "80-a0", "a0-c0", "c0-e0", "e0-",
	}

	shards16 = []string{
		"-10", "10-20", "20-30", "30-40", "40-50", "50-60", "60-70", "70-80", "80-90", "90-a0", "a0-b0", "b0-c0", "c0-d0", "d0-e0", "e0-f0", "f0-",
	}

	shards32 = []string{
		"-05", "05-10", "10-15", "15-20", "20-25", "25-30", "30-35", "35-40", "40-45", "45-50", "50-55", "55-60", "60-65", "65-70", "70-75", "75-80",
		"80-85", "85-90", "90-95", "95-a0", "a0-a5", "a5-b0", "b0-b5", "b5-c0", "c0-c5", "c5-d0", "d0-d5", "d5-e0", "e0-e5", "e5-f0", "f0-f5", "f5-",
	}
)

func setup() error {
	log.Infof("cluster.NewCluster")

	clusterInstance = cluster.NewCluster(cell, "localhost")

	// Start topo server
	log.Infof("clusterInstance.StartTopo")
	err := clusterInstance.StartTopo()
	if err != nil {
		return err
	}

	// Start sharded keyspace
	sKeyspace := &cluster.Keyspace{
		Name:      sKs,
		SchemaSQL: sSchemaSQL,
		VSchema:   sVSchema,
	}

	log.Infof("clusterInstance.StartKeyspace")
	err = clusterInstance.StartKeyspace(*sKeyspace, shards4, 0, false)
	if err != nil {
		return err
	}

	// Start vtgate
	log.Infof("clusterInstance.StartVtgate")
	err = clusterInstance.StartVtgate()
	if err != nil {
		return err
	}

	vtParams = clusterInstance.GetVTParams(sKs)
	return nil
}

type Metrics struct {
	CPU struct {
		User    []float64
		Total   []float64
		Idle    []float64
		GCTotal []float64
	}

	Heap struct {
		AllocBytes   []float64
		AllocObjects []float64
	}

	Requests []float64
}

func aggregate[T uint64 | float64](values []T) []T {
	var datapoints []T
	for i := 1; i < len(values); i++ {
		datapoints = append(datapoints, values[i]-values[i-1])
	}
	return datapoints
}

func (m *Metrics) Dump(w io.Writer, benchmark, instance string, N int, period time.Duration) {
	cpu0 := aggregate(m.CPU.User)
	cpu1 := aggregate(m.CPU.Total)
	cpu2 := aggregate(m.CPU.Idle)
	cpu3 := aggregate(m.CPU.GCTotal)
	mem0 := aggregate(m.Heap.AllocBytes)
	mem1 := aggregate(m.Heap.AllocObjects)
	S := period.Seconds()

	for i := 0; i < len(cpu0); i++ {
		fmt.Fprintf(w, "Benchmark%s/%s-%d 1", benchmark, instance, N)
		fmt.Fprintf(w, " %.06f UserCPU/s", cpu0[i]/S)
		fmt.Fprintf(w, " %.06f CPU/s", cpu1[i]/S)
		fmt.Fprintf(w, " %.06f IdleCPU/s", cpu2[i]/S)
		fmt.Fprintf(w, " %.06f GCCPU/s", cpu3[i]/S)
		fmt.Fprintf(w, " %.06f KB/s", (mem0[i]/1024.0)/S)
		fmt.Fprintf(w, " %.06f allocs/s", mem1[i]/S)
		fmt.Fprintf(w, "\n")
	}
}

func (m *Metrics) updateFloat(mmap map[string]float64, key string, a *[]float64) {
	v, ok := mmap[key]
	if !ok {
		panic(fmt.Sprintf("missing metric: %v", key))
	}
	*a = append(*a, v)
}

func (m *Metrics) updateUint64(mmap map[string]float64, key string, a *[]uint64) {
	v, ok := mmap[key]
	if !ok {
		panic(fmt.Sprintf("missing metric: %v", key))
	}
	*a = append(*a, uint64(v))
}

func (m *Metrics) Update(mmap map[string]float64) {
	m.updateFloat(mmap, "/cpu/classes/gc/total:cpu-seconds", &m.CPU.GCTotal)
	m.updateFloat(mmap, "/cpu/classes/total:cpu-seconds", &m.CPU.Total)
	m.updateFloat(mmap, "/cpu/classes/idle:cpu-seconds", &m.CPU.Idle)
	m.updateFloat(mmap, "/cpu/classes/user:cpu-seconds", &m.CPU.User)
	m.updateFloat(mmap, "/gc/heap/allocs:bytes", &m.Heap.AllocBytes)
	m.updateFloat(mmap, "/gc/heap/allocs:objects", &m.Heap.AllocObjects)
}

func gatherTabletStats(metricsCache map[string]*Metrics) error {
	getStats := func(port int) (map[string]float64, error) {
		debugUrl := fmt.Sprintf("http://localhost:%d/debug/runtime/metrics", port)

		resp, err := http.Get(debugUrl)
		if err != nil {
			return nil, err
		}
		defer resp.Body.Close()

		var dict map[string]float64
		if err = json.NewDecoder(resp.Body).Decode(&dict); err != nil {
			return nil, err
		}

		return dict, nil
	}

	update := func(key string, metrics map[string]float64) {
		var (
			m  *Metrics
			ok bool
		)
		if m, ok = metricsCache[key]; !ok {
			m = &Metrics{}
			metricsCache[key] = m
		}
		m.Update(metrics)
	}

	for _, ks := range clusterInstance.Keyspaces {
		for _, shard := range ks.Shards {
			tmetrics, err := getStats(shard.PrimaryTablet().HTTPPort)
			if err != nil {
				return err
			}

			key := fmt.Sprintf("tablet.%s.%s.primary", ks.Name, shard.Name)
			update(key, tmetrics)
		}
	}

	tmetrics, err := getStats(clusterInstance.VtgateProcess.Port)
	if err != nil {
		return err
	}

	update("vtgate", tmetrics)
	return nil
}

func main() {
	var N int = 4
	var totalDuration = 2 * time.Minute
	var warmup = 30 * time.Second
	var period = 10 * time.Second

	flag.Parse()

	defer func() {
		if clusterInstance != nil {
			clusterInstance.Teardown()
		}
	}()

	if err := setup(); err != nil {
		log.Fatalf("setup failed: %v", err)
	}

	b := &OLTP{
		MaxRows:   10000,
		RangeSize: 100,
	}

	{
		log.Infof("Initializing benchmark...")
		conn, err := mysql.Connect(context.Background(), &vtParams)
		if err != nil {
			log.Fatal(err)
		}
		err = b.Init(conn)
		if err != nil {
			log.Fatal(err)
		}
		conn.Close()
	}

	ctx, cancel := context.WithTimeout(context.Background(), totalDuration)
	defer cancel()

	var wg *errgroup.Group
	wg, ctx = errgroup.WithContext(ctx)

	for i := 0; i < N; i++ {
		wg.Go(func() error {
			log.Infof("Connection %d...", i)
			conn, err := mysql.Connect(ctx, &vtParams)
			if err != nil {
				return err
			}
			defer conn.Close()

			log.Infof("Starting worker %d", i)
			return b.SumRanges(ctx, conn)
		})
	}

	metricsCache := make(map[string]*Metrics)

	wg.Go(func() error {
		time.Sleep(warmup)

		tick := time.NewTicker(period)
		defer tick.Stop()

		for {
			select {
			case <-ctx.Done():
				return nil
			case <-tick.C:
				log.Infof("Capturing stats...")
				if err := gatherTabletStats(metricsCache); err != nil {
					log.Errorf("failed: %v", err)
				}
			}
		}
	})

	if err := wg.Wait(); err != nil {
		log.Fatal(err)
	}

	for key, m := range metricsCache {
		k := strings.Split(key, ".")
		m.Dump(os.Stdout, "OLTP", k[0], N, period)
	}
}
