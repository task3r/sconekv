size=$1
timeout=30

docker stack deploy cassandra --compose-file stacks/cassandra-local.yml
for (( i=2; i <= $size; i=$i+1 )); do
    sleep $timeout;
    docker service scale cassandra_cluster=$i
    echo "Deployed container $i"
done

echo "Done."
