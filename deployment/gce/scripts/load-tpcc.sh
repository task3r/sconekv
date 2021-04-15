if [ $# -lt 3 ]; then
    echo "ERROR: Invalid arguments"
    echo "Usage: ./load.sh system warehouses workers"
    exit
fi

system=$1
cluster="$system"_cluster
warehouses=$2
workers=$3
cluster_nodes=$(docker-machine ssh "$system"-manager "source useful-commands.sh; swarm_ips $cluster" | sed "s/,/;/g")

eval $(docker-machine env "$system"-manager)

if [ $1 = "sconekv" ]; then
    echo "No need for keyspace setup"
elif [ $1 = "cassandra" ]; then
    task=$(docker ps | tail -1 | awk '{ print $1 }')
    docker exec -it $task cqlsh -e "drop keyspace tpcc; create keyspace tpcc with REPLICATION = {'class' : 'SimpleStrategy', 'replication_factor': 4 }; use tpcc; create table tpcc_table (tpcc_key varchar primary key, tpcc_value blob);"
elif [ $1 = "cockroach" ]; then
    task=$(docker ps | tail -1 | awk '{ print $1 }')
    docker exec -it $task cockroach sql --insecure -e "alter range default configure zone using num_replicas=4; drop database if exists tpcc cascade; create database tpcc; CREATE TABLE tpcc.tpcc_table (tpcc_key VARCHAR (255) PRIMARY KEY, tpcc_value BYTES);"
else
    echo "ERROR: Invalid system"
    exit
fi 
exit
echo "Sleeping..."
sleep 100
echo "Slept."
eval $(docker-machine env "$system"-client)

if [ $1 = "sconekv" ]; then
    docker run -t --network sconekv_sconenet us.gcr.io/scone-296713/tpcc java -jar TPCC.jar -s SconeKV -populate -r 0 -warmup 0 -w $workers -warehouses $warehouses -nodes "$cluster_nodes"
elif [ $1 = "cassandra" ]; then
    docker run -t --network cassandra_cassnet us.gcr.io/scone-296713/tpcc java -jar TPCC.jar -s Cassandra -populate -r 0 -warmup 0 -w $workers -warehouses $warehouses -nodes "$cluster_nodes"
elif [ $1 = "cockroach" ]; then
    docker run -t --network cockroach_roachnet us.gcr.io/scone-296713/tpcc java -jar TPCC.jar -s JDBC -populare -r 0 -warmup 0 -w $workers -warehouses $warehouses -nodes "$cluster_nodes"
else
    echo "ERROR: Invalid system"
    exit
fi 
