### HDFS Commands
```bash
To start HDFS: 
$ $HADOOP_HOME/sbin/start-dfs.sh
To stop HDFS: 
$ $HADOOP_HOME/sbin/stop-dfs.sh
```

### Yarn Commands
```bash
To start Yarn: 
$ $HADOOP_HOME/sbin/start-yarn.sh 
To stop Yarn: 
$ $HADOOP_HOME/sbin/stop-yarn.sh 
```

### Commands to interact with directories/files
```bash
Change directories: 
$ $HADOOP_HOME/bin/hadoop fs -ls / 
Create directory: 
$ $HADOOP_HOME/bin/hadoop fs -mkdir /<file_name>
Put file in HDFS: 
$ $HADOOP_HOME/bin/hadoop fs -put <file_name> /wc 
Get file from HDFS:
$ $HADOOP_HOME/bin/hadoop fs -get /output/<file_name>
```

### Execution
```bash
Execute jar: 
$ $HADOOP_HOME/bin/hadoop jar youJarFilepath WordCount /wc /output
```