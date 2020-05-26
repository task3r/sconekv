docker stop $(docker ps -a -q --filter ancestor="task3r/sconekv-node" --format="{{.ID}}")
