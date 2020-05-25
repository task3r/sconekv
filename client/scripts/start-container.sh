echo 'start sconekv client'
java "-agentlib:jdwp=transport=dt_socket,server=y,suspend=n,address=*:5005" -jar client-1.0-SNAPSHOT-jar-with-dependencies.jar
