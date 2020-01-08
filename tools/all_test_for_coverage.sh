#!/bin/bash

# Copyright 2019 The Vitess Authors.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

# These test uses excutables and launch them as process
# After that all tests run, here we are testing those

# All Go packages with test files.
# Output per line: <full Go package name> <all _test.go files in the package>*


### Execute go/test/endtoend testcase ###
source build.env

packages_with_tests=$(go list -f '{{if len .TestGoFiles}}{{.ImportPath}} {{join .TestGoFiles " "}}{{end}}' ./go/.../endtoend/... | sort)

cluster_tests=$(echo "$packages_with_tests" | grep -E "go/test/endtoend" | cut -d" " -f1)

# Run cluster test sequentially

for i in $cluster_tests
do
   echo "starting test for $i"
   go test  $i -v -p=1 -is-coverage=true || :
done

### Execute unit testcase ###

packages_with_all_tests=$(go list -f '{{if len .TestGoFiles}}{{.ImportPath}} {{join .TestGoFiles " "}}{{end}}' ./go/... | sort)
all_except_endtoend_tests=$(echo "$packages_with_all_tests" | grep -v "endtoend" | cut -d" " -f1 )

counter=0
for pkg in $all_except_endtoend_tests
do
   go test -coverpkg=vitess.io/vitess/go/... -coverprofile "/tmp/unit_$counter.out" $pkg -v -p=1 || :
   counter=$((counter+1))
done
