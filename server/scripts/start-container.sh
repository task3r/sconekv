echo 'start sconekv container'
java "-agentlib:jdwp=transport=dt_socket,server=y,suspend=n,address=*:5005" -jar server-1.0-jar-with-dependencies.jar
