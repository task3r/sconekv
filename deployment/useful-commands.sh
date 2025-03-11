#!/bin/bash

scone_tasks() {
    docker service ps sconekv_cluster -q
}


scone_deploy() {
    docker pull sconekv-node
    docker stack deploy sconekv --compose-file ~/deployment/sconekv.yml
    echo "Waiting 1min for membership to form..."
    sleep 60
    scone_ready
}

scone_stop() {
  docker stack rm sconekv
}

scone_task_ip() {
        docker inspect $1 -f {{.NetworksAttachments}} | grep -o '[0-9]*\.[0-9]*\.[0-9]*\.[0-9]*'
}

scone_ips() {
    echo -e "TASK\t\t\t\tIP\t\tNODE"
    for task in `scone_tasks`; do
        ip=`scone_task_ip $task`
        node_id=`docker inspect $task -f {{.NodeID}}`
        node_name=`docker inspect $node_id -f {{.Description.Hostname}}`
        echo -e "$task\t$ip\t$node_name"
    done
}

scone_single_log() {
    for x in `scone_tasks`; do
        docker service logs $x
    done
}

scone_ready() {
    [[ $# = 1 ]] && size=$1 || size=20
    readys=`scone_single_log 2> /dev/null | grep "ready\." | wc -l`
    if [[ $readys = $size ]]; then
        echo "READY"
    else
        echo "NOT READY"
    fi
}

scone_deployment_creation_date() {
    docker inspect sconekv_cluster -f {{.CreatedAt}} | sed 's/\..*//;s/\s/_/'
}

scone_logs() {
    dir=`scone_deployment_creation_date`
    mkdir $dir
    processes=()
    for task in `scone_tasks`; do
        docker service logs $task |& sed 's/.*|\s//' > $dir/`scone_task_ip $task`.log &
        processes+=($!)
    done
    for process in "${processes[@]}"; do
        wait "$process" &> /dev/null
    done
}

swarm_labels() {
  docker node ls -q | xargs docker node inspect   -f '{{ .ID }} [{{ .Description.Hostname }}]: {{ .Spec.Labels }}'
}
