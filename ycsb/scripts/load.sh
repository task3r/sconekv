#!/bin/bash
java -cp sconekv-binding-1.0-jar-with-dependencies.jar site.ycsb.Client -load -db pt.ulisboa.tecnico.sconekv.ycsb.SconeKVClient -P $WORKLOAD -threads $THREADS -s -p scone.tx_size=$LOAD_TX_SIZE
