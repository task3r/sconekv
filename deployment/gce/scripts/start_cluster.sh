#!/usr/bin/env bash
set -e

if [ $# -lt 3 ]; then
    echo "ERROR: Invalid arguments"
    echo "Usage: ./start_cluster.sh cluster_name cluster_size use_tracker"
    exit
fi
CLUSTER_SIZE=$2
TRACKER=$3
#MACHINE_TYPE="e2-standard-2"
MACHINE_TYPE="e2-highmem-4"
CLIENT_MACHINE_TYPE="e2-standard-8"
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

function start_tracker() {
    echo "Creating tracker...";
    docker-machine rm -f $TRACKER_NAME || true
    docker-machine create $TRACKER_NAME \
        -d google \
        --google-machine-image $CONF_GOOGLE_IMAGE \
        --google-machine-type $TRACKER_MACHINE_TYPE \
        --google-zone $GOOGLE_ZONE \
        --google-tags swarm-cluster \
        --google-project $GOOGLE_PROJECT

    eval $(docker-machine env $TRACKER_NAME)
    eval $MGR_WORKER_JOIN_CALL
    eval $MGR_ENV
    docker node update --label-add sconekv_type=tracker $TRACKER_NAME
}

# Deploy manager
docker-machine rm -f $MANAGER_NAME || true
docker-machine create $MANAGER_NAME \
    -d google \
    --google-machine-image $CONF_GOOGLE_IMAGE \
    --google-machine-type $MACHINE_TYPE \
    --google-zone $GOOGLE_ZONE \
    --google-tags swarm-cluster \
    --google-project $GOOGLE_PROJECT

docker-machine scp -r configs $MANAGER_NAME:.
docker-machine scp useful-commands.sh $MANAGER_NAME:.
docker-machine ssh $MANAGER_NAME "echo 'source useful-commands.sh' >> .bashrc"
docker-machine ssh $MANAGER_NAME $INSTALL_DSTAT
MGR_ENV=$(docker-machine env $MANAGER_NAME)
eval $MGR_ENV
docker swarm init
MGR_WORKER_JOIN_CALL=$(docker swarm join-token worker | grep "docker swarm join")
docker node update --label-add sconekv_type=worker $MANAGER_NAME

# Deploy workers
((CLUSTER_SIZE--))
for i in $(seq 1 $CLUSTER_SIZE); do
    start_worker $i &
done

# Deploy client
start_client &

[ "$TRACKER" -eq 1 ] && start_tracker &

wait

echo "Docker swarm ready."
echo "$MGR_ENV"
