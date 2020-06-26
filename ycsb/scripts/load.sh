#!/bin/bash
java -cp sconekv-binding-1.0-SNAPSHOT-jar-with-dependencies.jar site.ycsb.Client -load -db pt.ulisboa.tecnico.sconekv.ycsb.SconeKVClient -P $WORKLOAD