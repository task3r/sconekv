#!/bin/bash


start_dstat() {
    docker-machine ssh $1 "dstat -t -c -m -d -n --noupdate --output $2.csv 1 400 > /dev/null &"
    echo "Started dstat $1 $2"
}

if [ $# -lt 5 ]; then
    echo "ERROR: Invalid arguments"
    echo "Usage: ./collect-resources.sh system size workload threads use_uniform"
    exit
fi

system=$1
size=$2
let cluster_size=size*4
let recordcount=cluster_size*50000
workload=$3
threads=$4
distribution=$5
cluster_nodes=""
time=$(date +%Y%m%d-%H%M%S)
test_basename=""
distribution=""
if [ $5 -eq 1 ]; then
    test_basename="$system-$cluster_size-$workload-$distribution-$threads-$time"
    distribution="uniform"
else
    test_basename="$system-$cluster_size-$workload-$threads-$time"
    distribution="zipfian"
fi

if [ $1 = "sconekv" ]; then
    cluster_nodes=$(docker-machine ssh sconekv-manager "source useful-commands.sh; swarm_ips sconekv_cluster" | sed "s/,/;/g")
    run_command="docker run -t --network sconekv_sconenet us.gcr.io/scone-296713/sconekv-ycsb java -cp sconekv-binding-1.0-SNAPSHOT-jar-with-dependencies.jar site.ycsb.Client -t -db pt.ulisboa.tecnico.sconekv.ycsb.SconeKVClient -P $workload -threads $threads -s -p scone.tx_size=5 -p operationcount=10000000 -p recordcount=$recordcount -p requestdistribution=$distribution -p maxexecutiontime=300 -p scone.nodes=\"$cluster_nodes\"|& tee results/$test_basename.log"

elif [ $1 = "cassandra" ]; then
    cluster_nodes=$(docker-machine ssh cassandra-manager "source useful-commands.sh; swarm_ips cassandra_cluster")
    #run_command="docker run -t --network cassandra_cassnet us.gcr.io/scone-296713/ycsb ./bin/ycsb.sh run cassandra-cql -P \"workloads/$workload\"  -p operationcount=10000000 -p hosts=$cluster_nodes -p maxexecutiontime=300 -threads $threads -p cassandra.readconsistencylevel=\"QUORUM\" -p cassandra.writeconsistencylevel=\"QUORUM\" -p recordcount=$recordcount -p requestdistribution=$distribution -s |& tee results/$test_basename.log"
    run_command="docker run -t --network cassandra_cassnet us.gcr.io/scone-296713/ycsb ./bin/ycsb.sh run cassandra-cql -P \"workloads/$workload\"  -p operationcount=10000000 -p hosts=$cluster_nodes -p maxexecutiontime=300 -threads $threads -p cassandra.readconsistencylevel=\"QUORUM\" -p cassandra.writeconsistencylevel=\"QUORUM\" -p recordcount=1000000 -s |& tee results/$test_basename.log"

elif [ $1 = "cockroach" ]; then
    cluster_nodes_ips=$(docker-machine ssh cockroach-manager "source useful-commands.sh; swarm_ips cockroach_cluster")
    cluster_nodes=$(for x in $(echo $cluster_nodes_ips | sed "s/,/ /g"); do printf "jdbc:postgresql://$x:26257/ycsb;"; done)
    run_command="docker run -t --network cockroach_roachnet us.gcr.io/scone-296713/ycsb ./bin/ycsb.sh run jdbc -P \"workloads/$workload\" -p operationcount=10000000 -p db.driver=org.postgresql.Driver -p db.url=\"$cluster_nodes\" -p db.user=root -p jdbc.autocommit=false -p maxexecutiontime=300 -threads $threads -p recordcount=$recordcount -p requestdistribution=$distribution -s |& tee results/$test_basename.log"

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

eval $(docker-machine env "$system"-client)
eval $run_command

gets=""
#sleep 100
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
