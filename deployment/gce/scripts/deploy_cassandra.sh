size=$1
timeout=45

eval $(docker-machine env cassandra-manager)
docker stack deploy cassandra --compose-file stacks/cassandra.yml
for (( i=2; i <= $size; i=$i+1 )); do
    sleep $timeout;
    docker service scale cassandra_cluster=$i
    echo "Deployed container $i"
done

echo "Done."
