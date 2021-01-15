eval $(docker-machine env cockroach-manager)
docker stack deploy cockroach --compose-file stacks/cockroach-$1.yml
sleep 20
eval $(docker-machine env $(docker service ps cockroach_seed | tail -1 | awk '{ print $4 }'))
seed_task=$(docker ps | grep cockroach_seed | awk '{ print $1 }')
docker exec $seed_task cockroach init --insecure
sleep 20
docker exec $seed_task cockroach node status --insecure
