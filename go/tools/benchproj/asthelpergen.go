/*
Copyright 2021 The Vitess Authors.

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
	"encoding/json"
	"flag"
	"fmt"
	"go/types"
	"os/exec"
	"regexp"
	"strings"
	"time"

	"golang.org/x/tools/go/packages"
)

type benchmark struct {
	filePath, path, name string
}

type benchmarkRunLine struct {
	Time    time.Time
	Action  string
	Package string
	Output  string
	Elapsed string
}

var benchmarkResultRe = regexp.MustCompile(`Benchmark.+\b\s*([0-9]+)\s+(\d+) ns/op`)

func main() {

	var pattern string

	flag.StringVar(&pattern, "in", "", "Go package to benchmark")
	flag.Parse()

	loaded, err := packages.Load(&packages.Config{
		Mode:  packages.NeedName | packages.NeedTypes | packages.NeedTypesInfo | packages.NeedDeps | packages.NeedImports | packages.NeedModule,
		Tests: true,
	}, pattern)
	if err != nil {
		panic(err)
	}

	benchmarks := findBenchmarks(loaded)

	for _, benchmark := range benchmarks {
		idx := strings.LastIndex(benchmark.filePath, "/")
		path := benchmark.filePath[:idx+1]
		command := exec.Command("go", "test", "-bench=^"+benchmark.name+"$", "-run==", "-json", "-count=10", path)
		b, err := command.Output()

		if err == nil {
			lines := strings.Split(string(b), "\n")
			for _, line := range lines {
				var benchLine benchmarkRunLine
				err := json.Unmarshal([]byte(line), &benchLine)
				if err != nil {
					continue
				}
				submatch := benchmarkResultRe.FindStringSubmatch(benchLine.Output)
				if len(submatch) > 0 {
					fmt.Printf("%s %s ns/op\n", benchmark.name, submatch[2])
				}

			}
		} else {
			fmt.Println(err.Error())
		}
	}
}

func findBenchmarks(loaded []*packages.Package) []benchmark {
	var benchmarks []benchmark
	for _, pkg := range loaded {
		scope := pkg.Types.Scope()
		for _, typName := range scope.Names() {
			f, ok := scope.Lookup(typName).(*types.Func)
			if ok && isBenchmark(f) {
				fs := pkg.Fset.File(f.Pos())
				benchmarks = append(benchmarks, benchmark{
					path:     f.Pkg().Path(),
					name:     f.Name(),
					filePath: fs.Name(),
				})
			}
		}
	}
	return benchmarks
}

func isBenchmark(f *types.Func) bool {
	return strings.HasPrefix(f.Name(), "Bench") && f.Type().String() == "func(b *testing.B)"
}
