# Hadoop and Spark Installation Guide

A comprehensive guide to installing and configuring Apache Hadoop and Apache Spark on Ubuntu/Debian systems, including a complete MapReduce WordCount example.

## Table of Contents

- [Prerequisites](#prerequisites)
- [System Setup](#system-setup)
- [Hadoop Installation](#hadoop-installation)
- [Hadoop Configuration](#hadoop-configuration)
- [Spark Installation](#spark-installation)
- [Starting Hadoop Services](#starting-hadoop-services)
- [Running WordCount Example](#running-wordcount-example)
- [Troubleshooting](#troubleshooting)
- [Useful Commands](#useful-commands)

## Prerequisites

- Ubuntu/Debian Linux (tested on Ubuntu 20.04+)
- Sudo privileges
- Internet connection for downloading packages

## System Setup

### Step 1: Install Java and SSH

Java 8 is required for Hadoop 3.3.6 compatibility.

```bash
sudo apt update
sudo apt install openjdk-8-jdk -y
sudo apt install ssh -y
```

Verify Java installation:
```bash
java -version
```

### Step 2: Create Hadoop User

Create a dedicated user for Hadoop operations:

```bash
sudo adduser hadoop
sudo usermod -aG sudo hadoop
```

Switch to the hadoop user:
```bash
su - hadoop
```

### Step 3: Configure SSH Passwordless Authentication

Generate SSH key pair:
```bash
ssh-keygen -t rsa -P '' -f ~/.ssh/id_rsa
```

Add the public key to authorized keys:
```bash
cat ~/.ssh/id_rsa.pub >> ~/.ssh/authorized_keys
chmod 0600 ~/.ssh/authorized_keys
```

Test SSH connection:
```bash
ssh localhost
```

Type `yes` when prompted, then `exit` to return to your session.

## Hadoop Installation

### Step 1: Download and Extract Hadoop

```bash
wget https://dlcdn.apache.org/hadoop/common/hadoop-3.3.6/hadoop-3.3.6.tar.gz
tar -xvzf hadoop-3.3.6.tar.gz
mv hadoop-3.3.6 hadoop
```

### Step 2: Set Hadoop Environment Variables

Add Hadoop paths to `.bashrc`:

```bash
cat >> ~/.bashrc << 'EOF'
export JAVA_HOME=/usr/lib/jvm/java-8-openjdk-amd64
export HADOOP_HOME=/home/hadoop/hadoop
export HADOOP_INSTALL=$HADOOP_HOME
export HADOOP_MAPRED_HOME=$HADOOP_HOME
export HADOOP_COMMON_HOME=$HADOOP_HOME
export HADOOP_HDFS_HOME=$HADOOP_HOME
export HADOOP_YARN_HOME=$HADOOP_HOME
export HADOOP_COMMON_LIB_NATIVE_DIR=$HADOOP_HOME/lib/native
export PATH=$PATH:$HADOOP_HOME/sbin:$HADOOP_HOME/bin
export HADOOP_OPTS="-Djava.library.path=$HADOOP_HOME/lib/native"
export HADOOP_CLASSPATH=$(hadoop classpath)
EOF
```

Apply the changes:
```bash
source ~/.bashrc
```

## Hadoop Configuration

### Step 1: Configure Java Home in Hadoop

Find the Java installation path:
```bash
readlink -f $(which java) | sed "s:/bin/java::"
```

Edit `hadoop-env.sh`:
```bash
nano $HADOOP_HOME/etc/hadoop/hadoop-env.sh
```

Set `JAVA_HOME` (around line 54):
```bash
export JAVA_HOME=/usr/lib/jvm/java-8-openjdk-amd64
```

### Step 2: Create HDFS Directories

```bash
mkdir -p ~/hadoopdata/hdfs/{namenode,datanode}
```

### Step 3: Configure core-site.xml

```bash
nano $HADOOP_HOME/etc/hadoop/core-site.xml
```

Add within `<configuration>` tags:
```xml
<configuration>
    <property>
        <name>fs.defaultFS</name>
        <value>hdfs://localhost:9000</value>
    </property>
</configuration>
```

### Step 4: Configure hdfs-site.xml

```bash
nano $HADOOP_HOME/etc/hadoop/hdfs-site.xml
```

Add within `<configuration>` tags:
```xml
<configuration>
    <property>
        <name>dfs.replication</name>
        <value>1</value>
    </property>
    <property>
        <name>dfs.namenode.name.dir</name>
        <value>file:///home/hadoop/hadoopdata/hdfs/namenode</value>
    </property>
    <property>
        <name>dfs.datanode.data.dir</name>
        <value>file:///home/hadoop/hadoopdata/hdfs/datanode</value>
    </property>
</configuration>
```

### Step 5: Configure mapred-site.xml

```bash
nano $HADOOP_HOME/etc/hadoop/mapred-site.xml
```

Replace `amrita-Latitude-5410` with your hostname (use `hostname` command):

```xml
<configuration>
    <property>
        <name>mapreduce.framework.name</name>
        <value>yarn</value>
    </property>
    <property>
        <name>yarn.app.mapreduce.am.env</name>
        <value>HADOOP_MAPRED_HOME=/home/hadoop/hadoop</value>
    </property>
    <property>
        <name>mapreduce.map.env</name>
        <value>HADOOP_MAPRED_HOME=/home/hadoop/hadoop</value>
    </property>
    <property>
        <name>mapreduce.reduce.env</name>
        <value>HADOOP_MAPRED_HOME=/home/hadoop/hadoop</value>
    </property>
    <property>
        <name>mapreduce.jobhistory.address</name>
        <value>localhost:10020</value>
    </property>
    <property>
        <name>mapreduce.jobhistory.webapp.address</name>
        <value>localhost:19888</value>
    </property>
</configuration>
```

### Step 6: Configure yarn-site.xml

```bash
nano $HADOOP_HOME/etc/hadoop/yarn-site.xml
```

```xml
<configuration>
    <property>
        <name>yarn.application.classpath</name>
        <value>$HADOOP_HOME/share/hadoop/common/*,$HADOOP_HOME/share/hadoop/common/lib/*,$HADOOP_HOME/share/hadoop/hdfs/*,$HADOOP_HOME/share/hadoop/hdfs/lib/*,$HADOOP_HOME/share/hadoop/mapreduce/*,$HADOOP_HOME/share/hadoop/mapreduce/lib/*,$HADOOP_HOME/share/hadoop/yarn/*,$HADOOP_HOME/share/hadoop/yarn/lib/*</value>
    </property>
    <property>
        <name>yarn.nodemanager.aux-services</name>
        <value>mapreduce_shuffle</value>
    </property>
    <property>
        <name>yarn.nodemanager.aux-services.mapreduce_shuffle.class</name>
        <value>org.apache.hadoop.mapred.ShuffleHandler</value>
    </property>
    <property>
        <name>yarn.log-aggregation-enable</name>
        <value>true</value>
    </property>
    <property>
        <name>yarn.log.server.url</name>
        <value>http://localhost:19888/jobhistory/logs</value>
    </property>
    <property>
        <name>yarn.timeline-service.enabled</name>
        <value>true</value>
    </property>
    <property>
        <name>yarn.nodemanager.remote-app-log-dir</name>
        <value>/tmp/logs</value>
    </property>
    <property>
        <name>yarn.nodemanager.remote-app-log-dir-suffix</name>
        <value>logs</value>
    </property>
    <property>
        <name>yarn.log-aggregation.file-formats</name>
        <value>TFile</value>
    </property>
    <property>
        <name>yarn.log-aggregation.retain-seconds</name>
        <value>-1</value>
    </property>
    <property>
        <name>yarn.timeline-service.hostname</name>
        <value>localhost</value>
    </property>
    <property>
        <name>yarn.timeline-service.address</name>
        <value>${yarn.timeline-service.hostname}:10200</value>
    </property>
    <property>
        <name>yarn.timeline-service.webapp.address</name>
        <value>${yarn.timeline-service.hostname}:8188</value>
    </property>
</configuration>
```

## Spark Installation

### Step 1: Install Python and PySpark

```bash
sudo apt install -y python3-pip
pip3 install pyspark --break-system-packages
```

### Step 2: Download and Install Spark

```bash
wget https://archive.apache.org/dist/spark/spark-3.5.0/spark-3.5.0-bin-hadoop3.tgz
tar -xvzf spark-3.5.0-bin-hadoop3.tgz
sudo mv spark-3.5.0-bin-hadoop3 /opt/spark
```

### Step 3: Set Spark Environment Variables

```bash
cat >> ~/.bashrc << 'EOF'
export SPARK_HOME=/opt/spark
export PATH=$PATH:$SPARK_HOME/bin:$SPARK_HOME/sbin
export PYSPARK_PYTHON=python3
EOF
```

Apply the changes:
```bash
source ~/.bashrc
```

Verify Spark installation:
```bash
spark-submit --version
```

## Starting Hadoop Services

### Step 1: Format HDFS (First Time Only)

**WARNING:** This will delete all data in HDFS. Only run on first installation or after complete reset.

```bash
hdfs namenode -format
```

### Step 2: Start Hadoop Services

```bash
start-all.sh
```

This starts:
- HDFS (NameNode, DataNode)
- YARN (ResourceManager, NodeManager)

### Step 3: Start JobHistoryServer

```bash
mapred --daemon start historyserver
```

### Step 4: Verify Services

Check running Java processes:
```bash
jps
```

You should see:
- NameNode
- DataNode
- ResourceManager
- NodeManager
- JobHistoryServer
- SecondaryNameNode

### Step 5: Access Web UIs

- **HDFS NameNode:** http://localhost:9870
- **YARN ResourceManager:** http://localhost:8088
- **JobHistoryServer:** http://localhost:19888

## Running WordCount Example

### Step 1: Create Test Input File

```bash
echo "We're up all night till the sun
We're up all night to get some 
We're up all night for good fun
We're up all night to get lucky" > lyrics.txt
```

### Step 2: Create HDFS Directory Structure

```bash
hdfs dfs -mkdir -p /user/hadoop/wordcount/input
```

**If you get an error about NameNode:**
```bash
# Stop all services
stop-all.sh

# Clear temporary data
rm -rf /tmp/hadoop-hadoop/*

# Reformat HDFS
hdfs namenode -format

# Restart services
start-all.sh
mapred --daemon start historyserver

# Try creating directory again
hdfs dfs -mkdir -p /user/hadoop/wordcount/input
```

### Step 3: Upload Input File to HDFS

```bash
hdfs dfs -put lyrics.txt /user/hadoop/wordcount/input
```

Verify the file:
```bash
hdfs dfs -ls /user/hadoop/wordcount/input
hdfs dfs -cat /user/hadoop/wordcount/input/lyrics.txt
```

### Step 4: Create WordCount Java Program

Get `WordCount.java`:

```
git clone git@github.com:Abhijeetk431/MLBD_Assignment1.git
```

Save and exit (Ctrl+X, then Y, then Enter).

### Step 5: Compile the Java Program

```bash
javac -classpath ${HADOOP_CLASSPATH} -d . WordCount.java
```

This creates `WordCount.class`, `WordCount$TokenizerMapper.class`, and `WordCount$IntSumReducer.class`.

### Step 6: Create JAR File

```bash
jar -cvf wordcount.jar WordCount*.class
```

### Step 7: Run the MapReduce Job

```bash
hadoop jar wordcount.jar WordCount /user/hadoop/wordcount/input /user/hadoop/wordcount/output
```

**Note:** The output directory must NOT exist before running the job.

### Step 8: View Results

```bash
hdfs dfs -cat /user/hadoop/wordcount/output/part-r-00000
```

Expected output:
```
We're   4
all     4
fun     1
get     2
good    1
lucky   1
night   4
some    1
sun     1
the     1
till    1
to      2
up      4
```

### Step 9: View Job Logs

List YARN applications:
```bash
yarn application -list -appStates ALL
```

View logs for a specific application (replace with your application ID):
```bash
yarn logs -applicationId application_1234567890123_0001
```

Filter logs for map and reduce tasks:
```bash
yarn logs -applicationId application_1234567890123_0001 | grep -E "\[MAP|\[REDUCE|===|TYPE"
```

## Re-running the Job

If you need to run the job again:

```bash
# Delete old output
hdfs dfs -rm -r /user/hadoop/wordcount/output

# Recompile (if you made changes)
javac -classpath ${HADOOP_CLASSPATH} -d . WordCount.java
jar -cvf wordcount.jar WordCount*.class

# Run the job
hadoop jar wordcount.jar WordCount /user/hadoop/wordcount/input /user/hadoop/wordcount/output
```

## Troubleshooting

### YARN Services Won't Start

If `start-yarn.sh` fails:

```bash
# Stop all services
stop-all.sh

# Remove PID files
rm /tmp/hadoop-hadoop-*.pid

# Clear temporary data and logs
rm -rf /tmp/hadoop-hadoop/*
rm -rf $HADOOP_HOME/logs/*

# Reformat namenode
hdfs namenode -format

# Start services
start-all.sh
mapred --daemon start historyserver
```

### Cannot Create HDFS Directory

If you get "Connection refused" or similar errors:

```bash
# Verify NameNode is running
jps | grep NameNode

# If not running, restart services
stop-all.sh
rm -rf /tmp/hadoop-hadoop/*
hdfs namenode -format
start-all.sh
```

### Output Directory Already Exists

MapReduce jobs fail if the output directory exists:

```bash
hdfs dfs -rm -r /user/hadoop/wordcount/output
```

### Check Service Status

```bash
# Check what's running
jps

# Check HDFS health
hdfs dfsadmin -report

# Check YARN nodes
yarn node -list
```

### View Logs

```bash
# Hadoop logs
tail -f $HADOOP_HOME/logs/hadoop-hadoop-namenode-*.log

# YARN logs
tail -f $HADOOP_HOME/logs/yarn-hadoop-resourcemanager-*.log

# JobHistoryServer logs
tail -f $HADOOP_HOME/logs/mapred-hadoop-historyserver-*.log
```

## Useful Commands

### HDFS Commands

```bash
# List files
hdfs dfs -ls /
hdfs dfs -ls /user/hadoop

# Create directory
hdfs dfs -mkdir -p /path/to/directory

# Upload file
hdfs dfs -put localfile.txt /hdfs/path/

# Download file
hdfs dfs -get /hdfs/path/file.txt localfile.txt

# View file contents
hdfs dfs -cat /path/to/file

# Delete file or directory
hdfs dfs -rm /path/to/file
hdfs dfs -rm -r /path/to/directory

# Check disk usage
hdfs dfs -du -h /

# File system check
hdfs fsck /
```

### Hadoop Service Management

```bash
# Start all services
start-all.sh

# Stop all services
stop-all.sh

# Start HDFS only
start-dfs.sh

# Stop HDFS only
stop-dfs.sh

# Start YARN only
start-yarn.sh

# Stop YARN only
stop-yarn.sh

# Start/stop individual services
hdfs --daemon start namenode
hdfs --daemon stop namenode
yarn --daemon start resourcemanager
yarn --daemon stop resourcemanager
```

### YARN Commands

```bash
# List applications
yarn application -list

# List all applications (including finished)
yarn application -list -appStates ALL

# Kill application
yarn application -kill <application_id>

# View application logs
yarn logs -applicationId <application_id>

# Check node status
yarn node -list
```

### JobHistoryServer

```bash
# Start JobHistoryServer
mapred --daemon start historyserver

# Stop JobHistoryServer
mapred --daemon stop historyserver

# Check status
jps | grep JobHistoryServer
```

## Directory Structure Reference

```
/home/hadoop/
├── hadoop/                         # Hadoop installation
│   ├── bin/                        # Hadoop binaries
│   ├── etc/hadoop/                 # Configuration files
│   │   ├── core-site.xml
│   │   ├── hdfs-site.xml
│   │   ├── mapred-site.xml
│   │   └── yarn-site.xml
│   ├── logs/                       # Hadoop logs
│   └── share/                      # Hadoop libraries
├── hadoopdata/hdfs/                # HDFS data storage
│   ├── namenode/                   # NameNode metadata
│   └── datanode/                   # DataNode blocks
└── /opt/spark/                     # Spark installation

HDFS Structure:
/user/hadoop/
└── wordcount/
    ├── input/                      # Input files
    │   └── lyrics.txt
    └── output/                     # Job output
        ├── _SUCCESS
        └── part-r-00000
```

## Port Reference

| Service | Port | URL |
|---------|------|-----|
| NameNode Web UI | 9870 | http://localhost:9870 |
| HDFS | 9000 | hdfs://localhost:9000 |
| ResourceManager Web UI | 8088 | http://localhost:8088 |
| JobHistoryServer | 19888 | http://localhost:19888 |
| Timeline Service | 8188 | http://localhost:8188 |
| DataNode Web UI | 9864 | http://localhost:9864 |
| NodeManager Web UI | 8042 | http://localhost:8042 |

## Next Steps

After completing this guide, you can:

1. **Explore PySpark:** Run the PySpark scripts from the main project
2. **Learn HDFS:** Practice file operations and understand distributed storage
3. **Write MapReduce Jobs:** Create custom MapReduce programs
4. **Configure Cluster:** Set up multi-node Hadoop cluster
5. **Integrate with Spark:** Run Spark jobs on YARN

## Additional Resources

- [Hadoop Official Documentation](https://hadoop.apache.org/docs/stable/)
- [HDFS Architecture Guide](https://hadoop.apache.org/docs/stable/hadoop-project-dist/hadoop-hdfs/HdfsDesign.html)
- [MapReduce Tutorial](https://hadoop.apache.org/docs/stable/hadoop-mapreduce-client/hadoop-mapreduce-client-core/MapReduceTutorial.html)
- [Apache Spark Documentation](https://spark.apache.org/docs/latest/)
- [YARN Architecture](https://hadoop.apache.org/docs/stable/hadoop-yarn/hadoop-yarn-site/YARN.html)
