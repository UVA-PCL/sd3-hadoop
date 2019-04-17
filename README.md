# SD3 Hadoop Research
Senior Capstone for Azman Garcha - Research into SD3 using Apache Hadoop

This code creates a simulation of the SD3 system based on multiple single-node clusters within the Apache Hadoop system. The first part of this simulation can be done independently of the node set-up.

PART 1 - PRELIMINARY SETUP

1. Use the RandomFileGenerator.java to generate random files for each cluster. The user must input the cluster number as well as the number of files for each cluster (1000 files per cluster is recommended to show the effect of the SD3 system). Each file will be named with file + number + extension (e.g., "file39.txt" corresponds to a file in cluster 3). The fileGenerator generates random text files with 100000 lines (used to simulate data artifacts of a significant, non-trivial, size)

2. Run TraceGenerator.java to generate random trace data. The files are stored in the ./cluster_file directory, and any custom directory can be specified through command-line arguments.

PART 2 - SETTING UP CLUSTERS/APACHE HADOOP

3. Setup clusters using whatever relevant data-nodes.

4. enable hdfs audit loging from the configuration file by:

In hadoop-env.sh:
export HDFS_AUDIT_LOGGER=INFO,RFAAUDIT
export HDFS_ROOT_LOGGER=INFO,RFA

In log4j.properties:
hdfs.audit.logger=INFO,RFAAUDIT
log4j.additivity.org.apache.hadoop.hdfs.AuditLogger=true

5. Upload the files to corresponding cluster and also upload one copy of trace data to each cluster.

6. Export ReadTrace.java to runnable jar files. Upload the jar file into each cluster.

7. About exporting the java file into jar files. I used eclipse.
1).First add the configuration. Right click on project -> Run As -> Run Configurations and create a config.
2).Then right click on the project in project explorer, select "Export". Then select "Runnable JAR file" under Java folder. Click "Next". Choose the "Launch configuration" as the Java file you want to export, such as "ReadTrace - hdfs" or "UpdateFiles - hdfs". Choose export Destination. Click "Finish". Then you will get two runnable Jar files.

PART 3 - STEPS FOR RUNNING JARS:

8. Run ./scripts/autoSD <Cluster_Index> <Trace_File_Path> <Cluster1_IP> <Cluster2_IP> <Cluster3_IP>". It will iteratively run the SD program 5 times and save the experimental results into ./Selective_Data_Experiment_Result directory.

9. The program will measure: performance without replication; performance with paritial replication; performance with complete replication.
