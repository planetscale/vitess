#!/bin/bash

# Copyright 2022 The Vitess Authors.
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
source build.env

# All Go packages with test files.
# Output per line: <full Go package name> <all _test.go files in the package>*
packages_with_tests=$(go list -f '{{if len .TestGoFiles}}{{.ImportPath}} {{join .TestGoFiles " "}}{{end}}' ./go/boost/.../endtoend/... | sort | cut -d" " -f1 | tr '\n' ' ')

# Run non-flaky tests.
go test -count=1 $packages_with_tests "$@" -gtid-mode=TRACK_GTID
if [ $? -ne 0 ]; then
  echo "ERROR: Go unit tests failed. See above for errors."
  exit 1
fi
