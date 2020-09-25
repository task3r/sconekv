#!/bin/bash

# usage ./collect-cpu.sh nodes client proxy test_name command

start_dstat() {
  ssh -J $2 $1 "vagrant ssh -c \"dstat -c 5 > $3 &\""
  echo "Started dstat $1 $3"
}

stop_dstat() {
  ##out=$(ssh -J "$2" "$1" "vagrant ssh -c \"pgrep -f dstat | xargs kill; cat $3 | awk '{ if (NR>2) { SUM += \\\$3; COUNT += 1 } } END { print SUM/COUNT }'\"")
  ssh -J "$2" "$1" "vagrant ssh -c \"pgrep -f dstat | xargs kill\""
  echo "Stopped dstat $1 $3"
}

nodes=`echo $1 | sed "s/,/ /g"`
client=$2
proxy=$3
test_name=$4

starts=""
for node in $nodes; do
    start_dstat "$node" "$proxy" "$test_name" &
    starts="$starts $!"
done
for pid in $starts; do
    wait $pid
done

ssh -J $proxy $client "vagrant ssh -c \"$5\""

count=0
sum=0
stops=""
for node in $nodes; do
    stop_dstat "$node" "$proxy" "$test_name" &
    stops="$stops $!"
    #sum=$(echo $sum+$result | bc)
    #((count++))
done
for pid in $stops; do
    wait $pid
done

#final=$(echo $sum/$count | bc)

#echo "CPU idle: $final"


