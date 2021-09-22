#!/bin/sh

kubectl port-forward --address localhost "$(kubectl get service --selector="planetscale.com/component=vtctld" -o name | head -n1)" 15000 15999 &
process_id1=$!
kubectl port-forward --address localhost "$(kubectl get service --selector="planetscale.com/component=vtgate,!planetscale.com/cell" -o name | head -n1)" 15306:3306 &
process_id2=$!
kubectl port-forward --address localhost "$(kubectl get service --selector="planetscale.com/component=vtgate,!planetscale.com/cell" -o name | head -n1)" 15001:15000 &
process_id3=$!
kubectl --namespace monitoring port-forward svc/grafana 3000 &
process_id4=$!
kubectl --namespace monitoring port-forward svc/prometheus-k8s 9090 &
process_id5=$!
sleep 2
echo "You may point your browser to http://localhost:15000, use the following aliases as shortcuts:"
echo 'alias vtctlclient="vtctlclient -server=localhost:15999 -logtostderr"'
echo 'alias mysql="mysql -h 127.0.0.1 -P 15306 -u user"'
echo "Hit Ctrl-C to stop the port forwards"
wait $process_id1
wait $process_id2
wait $process_id3
wait $process_id4
wait $process_id5
