FROM adoptopenjdk/openjdk13
ENV LANG C.UTF-8

WORKDIR /usr/src/sconekv
COPY target/sconekv-binding-1.0-SNAPSHOT-jar-with-dependencies.jar /usr/src/sconekv
COPY client-config.properties /usr/src/sconekv/client-config.properties
COPY scripts/cli.sh /usr/src/sconekv/cli
ENV PATH="${PATH}:/usr/src/sconekv"
CMD "/bin/bash"