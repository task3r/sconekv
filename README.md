# SconeKV
## Strongly Consistent Key-Value Store

# Dependencies
- Maven >= 3.6.3
- Java 13

# Build
Install dependencies:
```
mvn install:install-file -Dfile=deps/prime-1.0-SNAPSHOT-jar-with-dependencies.jar \
    -DgroupId=pt.tecnico.ulisboa \
    -DartifactId=prime \
    -Dversion=1.0-SNAPSHOT \
    -Dpackaging=jar
```
Clean build:
```
mvn clean package
```
Build containers:
```
./build_containers.sh
```
