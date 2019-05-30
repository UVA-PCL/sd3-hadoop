This is repository includes an implementation for experiments demonstrating
Lin, Shen, and Chandler, "Selective Data Replication for Online Social Networks with Distributed Datacenters" built on top of Hadoop HDFS.

To simulate the idea of having multiple distinct clusters, this prototype assumes that each cluster is its own HDFS instance (namenode and datanodes).
Data is replicated on the granularity of Hadoop files, and each file has a master copy, stored on a cluter identified by its name.

To access replicated copies, an agent on one of the clusters, first tries to access the file locally, then tries to access it on its master copy.
On each cluster, a thread monitors the Hadoop audit log and replicates files in accordance with the SD3-determined threshold.
In this implementation, these agents play back a sequence of accesses specified in a trace file.

# Setup 

To use the tools in this repository

*  build the Java portions with `mvn package`
*  copy `scripts/config.sh.template` to `scripts/config.sh` and edit itxi
   to correspond to your site and where you intend to run experiments. You should have passwordless
   SSH access to each machine on which you want to run a cluster. The scripts assume that all nodes have access to the 
   scripts via the same full path.
*  in the `config-base directory,  copy `hadoop-env.sh.template` to `hadoop-env.sh`, editing (at least)
   the `JAVA_HOME` setting. You may also wish to edit other files in this directory.


# Scripts

Included in this repository:

*  a script to setting up an single-node Hadoop cluster with an appropriate configuration file `scripts/setup-all`.
   This assumes that the scripts directory is accessible on all the machines to be used for single-node clusters
   (either it is on a shared network filesystem, or you copy over this repository manually). This
   assumes you place a Hadoop binary tarball for it to extract in a location specified in `scripts/config.sh`.

*  a script to starting and stopping the Hadoop clusters configured with `setup-all` in `scripts/start-all` and
   `scripts/stop-all`

*  a tool for generating random test files `scripts/file-generator` (Java code in sd3.RandomFileGenerator). This
   will generate the files for each cluster on each cluster separately.

*  a script for uploading the random test files, `scripts/upload-all`.  This requires to the generated files directory to
   be copied to or available on each node.

*  a tool for generating a pseudorandom trace indicating which files are read from which cluster, that reads some
    files preferentially according to a distribution, `scripts/trace-generator` (Java code in sd3.TraceGenerator).

   This prompts for its parameters. The resulting trace will include lines indicating a cluster number to perform an operation,
   a filename, and whether the operation is a read or write.
   The generated trace will preferentially choose lower-numbered files to access and each cluster will access
   files distributed across the clusters with equal probability.
   The trace will be placed in the directory configured
   in config.sh, which will need to be accessible on all nodes.

*  a tool for running  a trace simultaneously across each of the clusters, run as 

        scripts/all-read-trace TRACENAME OUTPUT-FILE-BASE
  
   where TRACENAME is the name of the trace file generated with scripts/trace-generator, 
   which should be the location configured in config.sh.
   and OUTPUT-FILE is the absolute path where the experiment output will be located.
   This output will include statitsics like the portion of local requests and the amount of data transferred locally
   and remotely.
   The bandwidth includes the amount data read or written (locally or remotely) when replaying the trace, but the
   bandwidth for replication is accounted to by the cluster replicated from only.
   Each cluster will write their own output file postfixed with `.1`, `.2`, etc.
   (Java code in `sd3.ReadTrace`)

   By default, this runs trials of the trace with different replication policies, deleting any existing replicas in between trials.
   Change the `main()` function sd3.ReadTrace to edit this behavior.

   Although the generated trace files interleave operations from each cluster, the current replay of the trace does not
   try to keep the different clusters explicitly in sync, so one cluster may start operations later in the trace
   than another cluster.

*  a tool for erasing extra replicas of files in `scripts/delete-extra` (Java code in sd3.DeleteExtra)

*  a tool for running the replication alone in `scripts/all-update-files`, which can then be killed by `scripts/kill-update-files`.
   (Java code in sd3.UpdateFiles)

# Note on replication implementation

The replication implementation assumes it can identify the source of an access to a file based on the IP address and that this IP
address is the same as the IP address of the namenode of the cluster from which that access comes. It uses HDFS's access log to
monitor accesses without running a modified version of HDFS.
To implement a different policy change the utility functions in sd3.SD3Config.

By default the replication implementation aims to find a cost threshold that causes a certain portion of files to be replicated,
and by default it checks every 10 seconds for files to replica, computing cost thresholds on a 1-hour interval. These settings
are configurable using system properties:

*  `sd3.replciate-interval`: seconds between scanning active files to determine what to replicate
*  `sd3.replicate-history-interval`: seconds of historical access to consider when determining what to replicate
*  `sd3.replicate-target-portion-enabled` (default: true): if set to true, choose replication threshold to replicate at least
    `sd3.replicate-target-portion` of the total files per cluster (e.g. 0.5 = try to replicate a whole cluster's worth of files);
     otherwise use `sd3.replicate-threshold` to as the cost threshold (units: # file transfers) to determine what files to replicate.

## Simplifications of algorithm for this context

*  The replication implementation does not attempt to manage the total storage used for replicas. Updates to master copies
   trigger deletion of replicas, but do not trigger creation of replicas. Rereplication is triggered because the copies
   still exceed the cost.

*  Data movement costs are between clusters are assumed uniform.

*  File size is assumed to be approximately uniform (and is uniform for this experiment setup) to simplify replica threshold calculation.

# Authors

The code in this repository was created by Azman Garcha,  Weiqiang Jin, and Zeitan Liu under the supervision of Charles Reiss
and Haiying Shen.

# Acknowledgements

We would like to thank the collaborators of this project:

*  Bruce Maggs, Pelham Wilder Professor of Computer Science, Duke University and Vice President, Research, Akamai Technologies
*  Weijia Xu, Research Engineer, Texas Advanced Computing Center 
*  Open Science Grid (OSG)
*  SURA (Southeastern Universities Research Association) 
*  Naval Research Lab
