# HadoopResearch
Senior Capstone for Azman Garcha - Research into SD3 using Apache Hadoop

STEPS FOR GIT (TERMINAL): 

-To clone it, go to the directory you want to clone it in, and type "git clone https://github.com/ozsingh/HadoopResearch.git"
-to pull (i.e., add changes from the main directory to your local copy), go to the directory that contains the git project in your terminal and type "git pull"
-to push any changes you've made after making said changes and testing locally, type three commands:
"git add .
git commit -m “commit message”
git push"

"commit message" includes whatever you want to label the commit message so it shows up on the git.

STEPS FOR RUNNING JARS (from Weiqiang):
1. Create 3 clusters.

2. enable hdfs audit loging from the configuration file by:

In hadoop-env.sh:
export HDFS_AUDIT_LOGGER=INFO,RFAAUDIT
export HDFS_ROOT_LOGGER=INFO,RFA

In log4j.properties:
hdfs.audit.logger=INFO,RFAAUDIT
log4j.additivity.org.apache.hadoop.hdfs.AuditLogger=true

3. Use the RandomFileGenerator.java to generate random files for each cluster. By default, it will generate 1000 files for each cluster. Name the files with file + number + extension. If the files are for cluster 1, the names will start with "1". If the files are for cluster 2, the names will start with "2", etc. When prompted, input the cluster number you want the data for.

4. Run TraceGenerator.java to generate random trace data. The files are stored in the ./cluster_file directory.

5. Upload the files to corresponding cluster and also upload one copy of trace data to each cluster.

6. Export ReadTrace.java to runnable jar files. Upload the jar file into each cluster.

7. About exporting the java file into jar files. I used eclipse. 
1).First add the configuration. Right click on project -> Run As -> Run Configurations and create a config.
2).Then right click on the project in project explorer, select "Export". Then select "Runnable JAR file" under Java folder. Click "Next". Choose the "Launch configuration" as the Java file you want to export, such as "ReadTrace - hdfs" or "UpdateFiles - hdfs". Choose export Destination. Click "Finish". Then you will get two runnable Jar files.

8. Run ./scripts/autoSD <Cluster_Index> <Trace_File_Path> <Cluster1_IP> <Cluster2_IP> <Cluster3_IP>". It will iteratively run the SD program 5 times and save the experimental results into ./Selective_Data_Experiment_Result directory.

9. The program will measure: performance without replication; performance with paritial replication; performance with complete replication.

