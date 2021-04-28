#!/bin/bash

start_dstat() {
    docker-machine ssh $1 "dstat -t -c -m -d -n --noupdate --output $2.csv 1 600 > /dev/null &"
    echo "Started dstat $1 $2"
}

if [ $# -lt 8 ]; then
    echo "ERROR: Invalid arguments"
    echo "Usage: ./run-tpcc.sh system size workers warehouses txs_per_worker container_to_kill node_with_task fault_type"
    exit
fi

system=$1
size=$2
let cluster_size=size*4
workers=$3
warehouses=$4
txs_per_worker=$5
container_to_kill=$6
node_with_task=$7
fault_type=$8
let n=workers*txs_per_worker
cluster="$system"_cluster
cluster_nodes=$(docker-machine ssh $system-manager "source useful-commands.sh; swarm_ips $cluster" | sed "s/,/;/g")
time=$(date +%Y%m%d-%H%M%S)
test_basename="tpcc-$system-$cluster_size-$workers-$warehouses-$n-fault-$fault_type-$time"

if [ $1 = "sconekv" ]; then
        run_command="docker run -t --network sconekv_sconenet us.gcr.io/scone-296713/tpcc java -jar TPCC.jar -s SconeKV -r 1 -warmup 0 -n $n -w $workers -warehouses $warehouses -nodes \"$cluster_nodes\" -status 1 |& tee results/$test_basename.log"
elif [ $1 = "cassandra" ]; then
    run_command="docker run -t --network cassandra_cassnet us.gcr.io/scone-296713/tpcc java -jar TPCC.jar -s Cassandra -r 1 -warmup 0 -n $n -w $workers -warehouses $warehouses -nodes \"$cluster_nodes\" -status 1 |& tee results/$test_basename.log"
elif [ $1 = "cockroach" ]; then
    run_command="docker run -t --network cockroach_roachnet us.gcr.io/scone-296713/tpcc java -jar TPCC.jar -s JDBC -r 1 -warmup 0 -n $n -w $workers -warehouses $warehouses -nodes \"$cluster_nodes\" -status 1 |& tee results/$test_basename.log"
else
    echo "ERROR: Invalid system"
    exit
fi 

starts=""
((size--))
for node in $(seq 1 $size); do
    start_dstat "$system-worker-$node" "$test_basename-$node" &
    starts="$starts $!"
done
start_dstat $system-manager "$test_basename"-0 &
starts="$starts $!"
start_dstat $system-client "$test_basename"-client &
starts="$starts $!"
for pid in $starts; do
    wait $pid
done

eval $(docker-machine env $system-client)
eval $run_command &
client="$!"
eval $(docker-machine env $system-"$node_with_task")
sleep 30
docker kill $container_to_kill
date >> results/$test_basename.log

wait $client


gets=""
for node in $(seq 1 $size); do
    docker-machine scp "$system-worker-$node":"$test_basename"*.csv resources &
    gets="$gets $!"
done
docker-machine scp  $system-manager:"$test_basename"*.csv resources & 
gets="$gets $!"
docker-machine scp $system-client:"$test_basename"*.csv resources &
gets="$gets $!"
for pid in $gets; do
    wait $pid
done

echo "Done."
