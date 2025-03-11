docker stop $(docker ps -a -q --filter ancestor="sconekv-node" --format="{{.ID}}")
