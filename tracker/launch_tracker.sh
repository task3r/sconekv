if [ ${PWD##*/} != "tracker" ]; then
    echo "Run this from the tracker directory"
    exit
fi

docker service rm tracker &> /dev/null
docker service create --name tracker --network sconenet tracker
tracker_port=$(docker inspect -f '{{range .NetworkSettings.Networks}}{{.IPAddress}}{{end}}' `docker ps | grep tracker | awk 'NR==1{print $1}'`)

echo $tracker_port

sed "s/tracker/$tracker_port/" ../server/config.properties > ../server/docker-config.properties
