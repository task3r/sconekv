#!/bin/bash
java -cp sconekv-binding-1.0-jar-with-dependencies.jar site.ycsb.Client -t -db pt.ulisboa.tecnico.sconekv.ycsb.SconeKVClient -P $WORKLOAD -threads $THREADS -s -p scone.tx_size=$BENCH_TX_SIZE -p operationcount=10000000 -p maxexecutiontime=300 |& tee "out/sconekv/"$WORKLOAD-$THREADS-$BENCH_TX_SIZE-`date +%Y%m%d-%H%M%S`.log
