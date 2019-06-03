'use strict';

function DemoController($scope, $http) {

  function init() {
    $scope.samples = [
        "select name, description from customer c join orders o on c.cid = o.cid join product p on o.pid = p.pid",
        "insert into product(pid, description) values(3, 'mouse')",
        "select m.mname, category, o.oid from merchant m join orders o on m.mname = o.mname",
        "update orders set mname='newegg' where oid=1",
        "select pid, description, amount from product join sales on product.pid = sales.pid",
        "select description, kount, amount from product join sales on product.pid = sales.pid order by amount desc limit 1",
        "insert into orders(oid, cid, mname, pid, price) values(4, 6, 'monoprice', 1, 50)",
    ];
    $scope.submitQuery()
  }

  $scope.submitQuery = function() {
    try {
      $http({
          method: 'POST',
          url: '/cgi-bin/data.py',
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

  $scope.setQuery = function($query) {
    $scope.query = $query;
    angular.element("#query_input").focus();
  };

  init();
}
