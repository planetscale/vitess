#!/usr/bin/env python
# coding: utf-8

# Copyright 2018 The Vitess Authors.
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

import environment
import utils
import vtgatev3_test

# Rerun all applicable vtgatev3_test cases in direct mode.

def setUpModule():
  environment.setup_protocol_flavor('direct')
  vtgatev3_test.transaction_mode = 'MULTI'
  vtgatev3_test.setUpModule()

def tearDownModule():
  vtgatev3_test.tearDownModule()

class TestVTGateDirectFunctions(vtgatev3_test.TestVTGateFunctions):

  # split query not supported by vtdirect.
  def test_split_query(self):
    pass

  # 2pc not supported by vtdirect.
  def test_transaction_modes(self):
    pass


if __name__ == '__main__':
  utils.main()
