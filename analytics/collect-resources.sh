#!/bin/bash


start_dstat() {
  ssh -J $2 $1 "vagrant ssh -c \"dstat -t -c -m -d -n --noupdate --output /vagrant/$3.csv 1 400 > /dev/null &\""
  echo "Started dstat $1 $3"
}

if [ $# -lt 6 ]; then
    echo "ERROR: Invalid arguments"
    echo "Usage: ./collect-resources.sh system nodes client proxy workload threads"
fi

system=$1
nodes=`echo $2 | sed "s/,/ /g"`
client=$3
proxy=$4
workload=$5
threads=$6
cluster_nodes=$7

if [ $1 = "sconekv" ]; then
    run_command="docker run -t --network sconekv_sconenet -v ~/client-logs:/usr/src/sconekv/out -e THREADS=$threads -e WORKLOAD=$workload task3r/sconekv-ycsb benchmark"
elif [ $1 = "cassandra" ]; then
    run_command="docker run -t --network cassandra_cassnet -v ~/client-logs:/usr/src/ycsb/out task3r/ycsb bash -c \\\"source useful_commands.sh; cassandra_run $workload $threads $cluster_nodes\\\""
elif [ $1 = "cockroach" ]; then
    run_command="docker run -t --network cockroach_roachnet -v ~/client-logs:/usr/src/ycsb/out task3r/ycsb bash -c \\\"source useful_commands.sh; cockroach_run $workload $threads $cluster_nodes\\\""
else
    echo "ERROR: Invalid system"
    exit
fi 

starts=""
time=`date +%Y%m%d-%H%M%S`
test_basename="$system-$time-$workload-$threads-"
for node in $nodes; do
    start_dstat "$node" "$proxy" "$test_basename$node" &
    starts="$starts $!"
done
start_dstat "$client" "$proxy" "$test_basename"client &
starts="$starts $!"
for pid in $starts; do
    wait $pid
done

ssh -J $proxy $client "vagrant ssh -c \"$run_command\""
