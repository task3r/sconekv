FROM adoptopenjdk/openjdk13
ENV LANG C.UTF-8

WORKDIR /usr/src/sconekv
COPY target/server-1.0-jar-with-dependencies.jar /usr/src/sconekv
COPY config.properties /usr/src/sconekv/config.properties
COPY scripts/start-container.sh /usr/src/sconekv
CMD ["/bin/bash", "start-container.sh"]
