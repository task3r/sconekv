#!/bin/bash
for size in {20,40,80}; do 
    for system in {cassandra,sconekv,cockroach}; do 
        n=0
        if [[ $system = "cockroach" ]] && [[ $size -eq 80 ]]; then
            echo "80 (0) cockroach skip"
            continue
        fi
        for x in `ls results/tpcc-"$system"-"$size"*`; do 
            ((n++))
            grep "STATUS" $x | awk -v test=$x 'NR>5 && NR<=35 {all+=$8; aborts+=$11} END {print test" "all" "aborts}' >> "$system"-"$size".tmp
        done
        awk -v size=$size -v n=$n -v s=$system '{all+=$2; aborts+=$3} END {print size" ("n") "s" "all/300/n" "(all-aborts)/300/n" "aborts/all*100"%"}' "$system"-"$size".tmp
        rm "$system"-"$size".tmp
    done
done
