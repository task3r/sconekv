if [ $# -lt 2 ]; then
    echo "ERROR: Invalid arguments"
    echo "Usage: ./load.sh system size"
    exit
fi

system=$1
cluster_nodes=""
let recordcount=$2*50000

eval $(docker-machine env "$system"-manager)

if [ $1 = "sconekv" ]; then
    echo "No need for keyspace setup"
    cluster_nodes=$(docker-machine ssh sconekv-manager "source useful-commands.sh; swarm_ips sconekv_cluster" | sed "s/,/;/g")
elif [ $1 = "cassandra" ]; then
    task=$(docker ps | tail -1 | awk '{ print $1 }')
    cluster_nodes=$(docker-machine ssh "$system"-manager "source useful-commands.sh; swarm_ips cassandra_cluster")
    docker exec -it $task cqlsh -e "drop keyspace ycsb; create keyspace ycsb with REPLICATION = {'class' : 'SimpleStrategy', 'replication_factor': 4 }; use ycsb; create table usertable (y_id varchar primary key,field0 varchar,field1 varchar,field2 varchar,field3 varchar,field4 varchar,field5 varchar,field6 varchar,field7 varchar, field8 varchar,field9 varchar);"
elif [ $1 = "cockroach" ]; then
    task=$(docker ps | tail -1 | awk '{ print $1 }')
    cluster_nodes_ips=$(docker-machine ssh "$system"-manager "source useful-commands.sh; swarm_ips cockroach_cluster")
    cluster_nodes=$(for x in $(echo $cluster_nodes_ips | sed "s/,/ /g"); do printf "jdbc:postgresql://$x:26257/ycsb;"; done)
    docker exec -it $task  cockroach sql --insecure -e "alter range default configure zone using num_replicas=4; drop database if exists ycsb cascade; create database ycsb; CREATE TABLE ycsb.usertable (YCSB_KEY VARCHAR (255) PRIMARY KEY, FIELD0 TEXT, FIELD1 TEXT, FIELD2 TEXT, FIELD3 TEXT, FIELD4 TEXT, FIELD5 TEXT, FIELD6 TEXT, FIELD7 TEXT, FIELD8 TEXT, FIELD9 TEXT);"
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
    docker run -t --network sconekv_sconenet us.gcr.io/scone-296713/sconekv-ycsb java -cp sconekv-binding-1.0-SNAPSHOT-jar-with-dependencies.jar site.ycsb.Client -load -db pt.ulisboa.tecnico.sconekv.ycsb.SconeKVClient -P workloada -threads 128 -s -p scone.tx_size=100 -p recordcount="$recordcount" -p scone.nodes="$cluster_nodes"
elif [ $1 = "cassandra" ]; then
    docker run -t --network cassandra_cassnet us.gcr.io/scone-296713/ycsb ./bin/ycsb.sh load cassandra-cql -P "workloads/workloada"  -p hosts="$cluster_nodes" -threads 128 -p recordcount=1000000 -s
elif [ $1 = "cockroach" ]; then
    docker run -t --network cockroach_roachnet us.gcr.io/scone-296713/ycsb ./bin/ycsb.sh load jdbc -P "workloads/workloada" -p db.driver=org.postgresql.Driver -p db.url="$cluster_nodes" -p db.user=root -p db.batchsize=2000 -p jdbc.autocommit=true -threads 64 -p recordcount="$recordcount" -s
else
    echo "ERROR: Invalid system"
    exit
fi 
