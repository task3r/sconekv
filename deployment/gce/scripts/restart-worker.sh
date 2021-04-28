#!/usr/bin/env bash
set -e

if [ $# -lt 2 ]; then
    echo "ERROR: Invalid arguments"
    echo "Usage: ./restart-worker.sh cluster_name worker_id"
    exit
fi

MACHINE_TYPE="e2-highmem-4"
CLIENT_MACHINE_TYPE="e2-standard-8"
TRACKER_MACHINE_TYPE="e2-standard-2"
CLUSTER_NAME="$1"
WORKER_ID="$2"
MANAGER_NAME="$CLUSTER_NAME-manager"
GOOGLE_PROJECT="scone-296713"
GOOGLE_ZONE="us-central1-f"
CONF_GOOGLE_IMAGE="https://www.googleapis.com/compute/v1/projects/ubuntu-os-cloud/global/images/family/ubuntu-1804-lts"
INSTALL_DSTAT="sudo apt install -y -qq dstat; sudo usermod -aG docker \$USER"

function start_worker() {
    local INDEX=$1
    local CUR_WORKER_NAME="$CLUSTER_NAME-worker-$INDEX";

    echo "Creating worker: $CUR_WORKER_NAME...";
    docker-machine rm -f  $CUR_WORKER_NAME || true
    docker-machine create $CUR_WORKER_NAME \
        -d google \
        --google-machine-image $CONF_GOOGLE_IMAGE \
        --google-machine-type $MACHINE_TYPE \
        --google-zone $GOOGLE_ZONE \
        --google-tags swarm-cluster \
        --google-project $GOOGLE_PROJECT
    
    docker-machine scp -r configs $CUR_WORKER_NAME:.
    docker-machine ssh $CUR_WORKER_NAME $INSTALL_DSTAT
    eval $(docker-machine env $CUR_WORKER_NAME)
    eval $MGR_WORKER_JOIN_CALL
    eval $MGR_ENV
    docker node update --label-add sconekv_type=worker $CUR_WORKER_NAME
}

MGR_ENV=$(docker-machine env $MANAGER_NAME)
eval $MGR_ENV
MGR_WORKER_JOIN_CALL=$(docker swarm join-token worker | grep "docker swarm join")

start_worker $WORKER_ID

echo "Docker swarm ready."
echo "$MGR_ENV"
