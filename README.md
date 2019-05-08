This is repository includes an implementation for experiments simulating
Lin, Shen, and Chandler, "Selective Data Replication for Online Social Networks with Distributed Datacenters" built on top of Hadoop HDFS.

To simulate the idea of having multiple distinct clusters, this prototype assumes that each cluster is its own HDFS instance (namenode and datanodes).
Data is replicated on the granualirty of Hadoop files, and each file has a master copy, stored on a cluter identified by its name,

To access replicated copies, an agent on one of the clusters, first tries to access the file locally, then tries to access it on its master copy.
On each cluster, a thread monitors the Hadoop audit log and replicates files in accordance with the SD3-determined threshold.

To use the tools in this repository

*  build the Java portions with `mvn package`
*  edit `scripts/config.sh` to correspond to your site and where you intend to run experiements. You should have passwordless
   SSH access to each machine on which you want to run a cluster.
  

Included in this repository:

*  a script to setting up an single-node hadoop cluster with an appropriate configuration file `scripts/setup-all`.
   This assumes that the scripts directory is accessible on all the machines to be used for single-node clusters
   (either it is on a shared network filesystem, or you copy over this repository manually). This
   assumes you place a Hadoop binary trball for it to extract in a location specified in `scripts/config.sh`.

*  a script to starting and stopping the Hadoop clusters configured with `setup-all` in `scripts/start-all` and
   `scripts/stop-all`

*  a tool for generating random test files `scripts/file-generator` (Java code in sd3.RandomFileGenerator). This prompts for its parameters.

*  a script for uploading the random test files, `scripts/upload-all`.

*  a tool for generating a psuedorandom trace indicating which files are read from which cluster, that reads some files preferentially according to a distribution,
   `scripts/trace-generator` (Java code in sd3.TraceGenerator).
   This prompts for its parameters. The resulting trace will include lines indicating a cluster number to perform an operation,
   a filename, and whether the operation is a read or write.

*  a tool for running  a trace simulatenously across each of the clusters, run as 

        script/run-all-trace TRACENAME OUTPUT-FILE-BASE
  
   where TRACENAME is the name of the trace file genreator with scripts/trace-generator, which should be in a location accessible on all nodes,
   and OUTPUT-FILE is where the experiment output will be located. Each cluster will write their own output file postfixed with `.1`, `.2`, etc.
   (Java code in `sd3.ReadTrace`)

   By default, this runs trials of the trace with different replication policies. Change the `main()` function sd3.ReadTrace to edit this behavior.

*  a tool for erasing extra replicas of files in `scripts/delete-extra` (Java code in sd3.DeleteExtra)

*  a tool for running the replication alone in `scripts/all-update-files`, which can then be killed by `scripts/kill-update-files`.
   (Java code in sd3.UpdateFiles)

   The replication implementation assumes it can identify the source of an access to a file based on the IP address and that this IP
   address is the same as the IP address of the namenode of the cluster from which that access comes. To implement a different policy
   change the utility functions in sd3.SD3Config. The code is also hard-coded to assume three clusters in the `main()` of sd3.ReadTrace
   and sd3.UpdateFiles.

# Authors

The code in this repository was created by Azman Garcha,  Weiqiang Jin, and Zeitan Liu under the supervision of Charles Reiss
and Haiying Shen.

# Acknowledgements

We would like to thank the collabarators of this project:

*  Bruce Maggs, Pelham Wilder Professor of Computer Science, Duke University and Vice President, Research, Akamai Technologies
*  Weijia Xu, Research Engineer, Texas Advanced Computing Center 
*  Open Science Grid (OSG)
*  SURA (Southeastern Universities Research Association) 
*  Naval Research Lab
