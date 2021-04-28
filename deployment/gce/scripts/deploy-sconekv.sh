size=$1
eval $(docker-machine env sconekv-manager)
docker stack deploy sconekv --compose-file stacks/sconekv-$size.yml
source useful-commands.sh
sleep 60
scone_ready $1
