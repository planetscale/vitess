/**
 * Copyright 2015, Google Inc. All rights reserved. Use of this source code is
 * governed by a BSD-style license that can be found in the LICENSE file.
 */
'use strict';

function DemoController($scope, $http) {

  function init() {
    $scope.submitQuery()
  }

  $scope.submitQuery = function() {
    try {
      $http({
          method: 'POST',
          url: '/exec',
          data: "query=" + $scope.query,
          headers: {
            'Content-Type': 'application/x-www-form-urlencoded'
          }
      }).success(function(data, status, headers, config) {
        $scope.result = angular.fromJson(data);
      });
    } catch (err) {
      $scope.result.error = err.message;
    }
  };

  $scope.submitProduct = function() {
    try {
      $http({
          method: 'POST',
          url: '/exec',
          data: "product=1&sku=" + $scope.sku+"&desc=" + $scope.desc+"&price=" + $scope.price,
          headers: {
            'Content-Type': 'application/x-www-form-urlencoded'
          }
      }).success(function(data, status, headers, config) {
        $scope.result = angular.fromJson(data);
      });
    } catch (err) {
      $scope.result.error = err.message;
    }
  };

  $scope.submitCustomer = function() {
    try {
      $http({
          method: 'POST',
          url: '/exec',
          data: "customer=1&name=" + $scope.name,
          headers: {
            'Content-Type': 'application/x-www-form-urlencoded'
          }
      }).success(function(data, status, headers, config) {
        $scope.result = angular.fromJson(data);
      });
    } catch (err) {
      $scope.result.error = err.message;
    }
  };

  $scope.submitOrder = function() {
    try {
      $http({
          method: 'POST',
          url: '/exec',
          data: "order=1&cid=" + $scope.cid+"&sku="+$scope.sku,
          headers: {
            'Content-Type': 'application/x-www-form-urlencoded'
          }
      }).success(function(data, status, headers, config) {
        $scope.result = angular.fromJson(data);
      });
    } catch (err) {
      $scope.result.error = err.message;
    }
  };

  init();
}
