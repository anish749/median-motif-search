# Median Motif String Search using Map Reduce

### Prerequisites
Maven, Java

### Compile and package
```
mvn clean package
```

### Arguments
 - Path to DNA sequences input file
 - Temporary path for intermediate files
 - Output path for final destination files
 - (Optional) Motif Length. Default is 8.


### Run as
```
haddop jar target/median-motif-search-1.0-SNAPSHOT.jar MedianStringMotifSearchMapReduce /input/path/hdfs/ /tmp/path/hdfs/ output/path/hdfs <motif length>
```