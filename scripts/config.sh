# location of maven executable
MAVEN=/zf14/cr4bd/bin/mvn

# location to place generated traces; this should be accessible
# from all nodes, or synchronized to them after traces are generated.
TRACE_DATA=/zf14/cr4bd/trace-data-dir
# location to place generated data files for traces before uploading them
GENERATED_FILES=/localtmp/generated-files-test

# location to place experiment related files, including Hadoop binaries
# and temporary directories. This will be subsituted in the Hadoop
# configuration templates (hadoop-env.sh, hdfs-site.xml, core-site.xml,
# log4j.properties) in this directory whereever @EXPERIMENT_DIR@ appears.
EXPERIMENT_DIR=/localtmp/cr4bd-test


# Hosts to run experiment on. The first will host "cluster 1", containing
# generated files with numbers starting 1, the second "cluster 2", and so on.
EXPERIMENT_HOSTS=(128.143.136.116 128.143.69.232 128.143.69.231)
# set EXPERIMENT_HOSTS_COMMAS to a comma-separated version of EXPERIMENT_HOSTS
IFS_OLD=$IFS
IFS=",";
EXPERIMENT_HOSTS_COMMAS="${EXPERIMENT_HOSTS[*]}"
IFS=$IFS_OLD

# Hadoop binary tarball to extract.
HADOOP_TGZ=/net/zf14/cr4bd/research/hadoop-2.9.2.tar.gz

# Location of audit log after copying configuration files
# hadoop-env.sh and log4j.properties this directory
AUDIT_LOG=$EXPERIMENT_DIR/hadoop-2.9.2/logs/hdfs-audit.log

# Options to pass when running Java. This should set system properties
# starting with sd3 for experiment settings, such as the location
# of the trace data, the HDFS port to expect, the audit log file
# location, and the port number to use for communication between
# daemons
JAVA_OPTS="-Dsd3.listener-port=22222 -Dsd3.hdfs-port=9001 \
           -Dsd3.trace-data=$TRACE_DATA -Dsd3.audit-log=$AUDIT_LOG \
           -Dsd3.generated-files=$GENERATED_FILES \
           -Dsd3.cluster-hosts=$EXPERIMENT_HOSTS_COMMAS"

# Other settings in system properties:
    # sd3.replicate-interval -- # seconds between checking for new replication tasks
    # sd3.replciate-history-interval -- amount of historical access information to 
    #                                   consider in determining whether the threshold
    #                                   for replication is met
    # sd3.trace-file-per-cluster -- # of files per cluster in synthetic traces


# Our current host name, for accessing the local HDFS installation from its namenode.
HOST=${HOST-$(hostname -i)}

# The port on which HDFS will run, as configured in core-site.xml
HDFS_PORT=9001

## JAVA_HOME, etc. setup
source /etc/profile.d/modules.sh
module load java9


### The rest of this is not intended to be edited
ROOT=$(realpath $(dirname $0)/..)
SCRIPT_DIR=$(realpath $(dirname $0))


function run_java() {
    class=$1
    shift
    MAVEN_OPTS="$JAVA_OPTS" $MAVEN -f $ROOT/pom.xml exec:java -Dexec.mainClass="sd3.$class" "-Dexec.args=${*}"
}
