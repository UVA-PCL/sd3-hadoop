# location of maven executable
MAVEN=/zf14/cr4bd/bin/mvn

# location to place generated trace data
TRACE_DATA=/localtmp/trace-data

# location to place experiment related files, including Hadoop binaries
# and temporary directories. This will be subsituted in the Hadoop
# configuration templates (hadoop-env.sh, hdfs-site.xml, core-site.xml,
# log4j.properties) in this directory whereever @EXPERIMENT_DIR@ appears.
EXPERIMENT_DIR=/localtmp/cr4bd

# Hosts to run experiment on. The first will host "cluster 1", containing
# generated files with numbers starting 1, the second "cluster 2", and so on.
#EXPERIMENT_HOSTS=(shen-16 shen-17 shen-18)
EXPERIMENT_HOSTS=(128.143.136.116 128.143.69.232 128.143.69.231)

# Hadoop binary tarball to extract.
HADOOP_TGZ=/net/zf14/cr4bd/research/hadoop-2.9.2.tar.gz

# Location of audit log after copying configuration files
# hadoop-env.sh and log4j.properties this directory
AUDIT_LOG=$EXPERIMENT_DIR/hadoop-2.9.2/logs/hdfs-audit.log

# Options to pass when running Java. This should set system properties
# starting with sd3 for experiment settings, such as the location
# of
JAVA_OPTS="-Dsd3.listener-port=22222 -Dsd3.hdfs-port=9001 -Dsd3.tracedata=$TRACE_DATA -Dsd3.audit-log=$AUDIT_LOG"

# Our current host name, for accessing the local HDFS installation from its namenode.
HOST=${HOST-$(hostname -i)}

# The port on which HDFS will run, as configured in core-site.xml
HDFS_PORT=9001


ROOT=$(realpath $(dirname $0)/..)
SCRIPT_DIR=$(realpath $(dirname $0))

source /etc/profile.d/modules.sh
module load java9

function run_java() {
    class=$1
    shift
    MAVEN_OPTS="$JAVA_OPTS" $MAVEN -f $ROOT/pom.xml exec:java -Dexec.mainClass="$class" "-Dexec.args=${*}"
}
