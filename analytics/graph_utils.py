#!/usr/bin/env python3
import sys
import re
import os
import matplotlib.pyplot as plt
from pathlib import Path

pattern = "^[0-9]+-[0-9]+-[0-9]+.*ops/sec.*"  # matches only the periodic ycsb lines with ops/sec


# plot throughput/time
def plot_benchmark(file_in, dir_prefix=None, combine=False):
    x = []
    y = []
    with open(file_in, 'r') as f:
        data = f.read()
        for line in re.findall(pattern, data, re.MULTILINE):
            split_line = line.split(" ")
            x.append(int(split_line[2]))
            y.append(float(split_line[6]))
    plt.plot(x, y)
    plt.ylabel('throughput/s')
    out_file = f"{dir_prefix}/{Path(file_in).stem}.eps" if dir_prefix is not None else f"{Path(file_in).stem}.eps"
    plt.savefig(out_file)
    if not combine:
        plt.clf()


if __name__ == '__main__':
    path_out, combine_plots = None, False
    if len(sys.argv) < 2:
        raise AttributeError("Invalid arguments, usage: graph_utils.py path_in [path_out] [combine_plots]")
    path_in = sys.argv[1]
    if len(sys.argv) >= 3:
        path_out = sys.argv[2]
    if len(sys.argv) >= 4:
        combine_plots = True
    if os.path.isdir(path_in):
        for file_name in os.listdir(path_in):
            file_path = os.path.join(path_in, file_name)
            plot_benchmark(file_path, path_out, combine_plots)
    elif os.path.isfile(path_in):
        plot_benchmark(path_in, path_out, combine_plots)

























