#!/bin/bash

get_logs() {
    eval $(docker-machine env $1)
    for c in `docker ps -q`; do
        docker cp $c:/usr/src/sconekv/logs/sconekv.log $c.log; #zip $c.zip $c.log
        #rm $c.log
    done
}

 for w in {1..4}; do
     get_logs sconekv-worker-$w &
 done

 get_logs sconekv-manager &

 wait

 echo "Done."
