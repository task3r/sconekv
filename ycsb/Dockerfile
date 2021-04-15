FROM adoptopenjdk/openjdk13
ENV LANG C.UTF-8

WORKDIR /usr/src/sconekv
COPY target/sconekv-binding-1.0-jar-with-dependencies.jar /usr/src/sconekv
COPY client-config.properties /usr/src/sconekv/client-config.properties
COPY scripts/cli.sh /usr/src/sconekv/cli
COPY scripts/benchmark.sh /usr/src/sconekv/benchmark
COPY scripts/load.sh /usr/src/sconekv/load
COPY workloads /usr/src/sconekv/
ENV PATH="${PATH}:/usr/src/sconekv"
ENV WORKLOAD="workloada"
ENV THREADS="128"
ENV LOAD_TX_SIZE="100"
ENV BENCH_TX_SIZE="5"
CMD "/bin/bash"
