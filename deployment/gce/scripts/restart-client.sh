#!/usr/bin/env bash
set -e

if [ $# -lt 1 ]; then
    echo "ERROR: Invalid arguments"
    echo "Usage: ./restart-client.sh cluster_name"
    exit
fi
MACHINE_TYPE="e2-highmem-4"
#CLIENT_MACHINE_TYPE="e2-standard-8"
CLIENT_MACHINE_TYPE="e2-standard-16"
TRACKER_MACHINE_TYPE="e2-standard-2"
CLUSTER_NAME="$1"
MANAGER_NAME="$CLUSTER_NAME-manager"
CLIENT_NAME="$CLUSTER_NAME-client"
TRACKER_NAME="$CLUSTER_NAME-tracker"
GOOGLE_PROJECT="scone-296713"
#GOOGLE_PROJECT="sconekv-scalability"
GOOGLE_ZONE="us-central1-f"
CONF_GOOGLE_IMAGE="https://www.googleapis.com/compute/v1/projects/ubuntu-os-cloud/global/images/family/ubuntu-1804-lts"
INSTALL_DSTAT="sudo apt install -y -qq dstat; sudo usermod -aG docker \$USER"

function start_client() {
    echo "Creating client...";
    docker-machine rm -f $CLIENT_NAME || true
    docker-machine create $CLIENT_NAME \
        -d google \
        --google-machine-image $CONF_GOOGLE_IMAGE \
        --google-machine-type $CLIENT_MACHINE_TYPE \
        --google-zone $GOOGLE_ZONE \
        --google-tags swarm-cluster \
        --google-project $GOOGLE_PROJECT

    docker-machine ssh $CLIENT_NAME $INSTALL_DSTAT
    eval $(docker-machine env $CLIENT_NAME)
    eval $MGR_WORKER_JOIN_CALL
    eval $MGR_ENV
    docker node update --label-add sconekv_type=client $CLIENT_NAME
}
MGR_ENV=$(docker-machine env $MANAGER_NAME)
eval $MGR_ENV
MGR_WORKER_JOIN_CALL=$(docker swarm join-token worker | grep "docker swarm join")

start_client

echo "$MGR_ENV"
