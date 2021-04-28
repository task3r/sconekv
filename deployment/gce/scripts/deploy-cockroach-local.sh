docker stack deploy cockroach --compose-file stacks/cockroach-$1.yml
sleep 5
seed_task=$(docker ps | grep cockroach_seed | awk '{ print $1 }')
docker exec $seed_task cockroach init --insecure
sleep 5
docker exec $seed_task cockroach node status --insecure
