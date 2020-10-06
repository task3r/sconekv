#! /usr/bin/env python3
import numpy as np
import os
import sys

percentiles = [5,25,50,75,90]

def get_percentiles_from_file(name):
    with open(name,'r') as f:
        lines = f.readlines()
        return np.percentile(list(map(float,lines)), percentiles)


if len(sys.argv) != 2:
    print("error")
    sys.exit()
else:
    metric = sys.argv[1]

os.system(f"./get-{metric}.sh")

for w in ['a','b','c','f']:
    for t in [16,32,64,128,256]:
        f = open(f"{metric}-workload{w}-{t}.dat", 'w')
        f.write(f"system 5 25 50 75 90\n")
        for system in ['cassandra', 'sconekv', 'cockroach']:
            file_name = f"{metric}/{metric}-{system}-workload{w}-{t}.dat"
            f.write(f"{system} ")
            p = get_percentiles_from_file(file_name)
            line = f"{p[0]}"
            for i in range(len(p)-1):
                difference = p[i+1] - p[i]
                line += f" {difference}"
            f.write(f"{line}\n")
        f.close()
    sys.exit()




