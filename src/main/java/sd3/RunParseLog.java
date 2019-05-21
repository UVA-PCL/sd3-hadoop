package sd3;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FileUtil;
import org.apache.hadoop.fs.Path;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.URI;
import java.util.ArrayList;
import java.util.Date;


public class RunParseLog implements Runnable {
    private static final boolean DEBUG = false;

    Cluster cluster;

    private Date curTime;
    private int interval;
    public boolean usePolicy;


    RunParseLog(Date curTime, int interval, Cluster c, boolean usePolicy) {
        this.cluster = c;
        this.curTime = curTime;
        this.interval = interval;
        this.usePolicy = usePolicy;
    }

    public void run() {
        try {
            curTime = new Date();
            ArrayList<String[]> trimmed_records = null;
            double thold = 0.0;
            double replica_percent = 0.0;
            double chosen_threshold = 0.0;
            double chosen_replica_percent = 0.0;
            ArrayList<String[]> chosen_trimmed_records = new ArrayList<String[]>();
            if (usePolicy) {
                ParseLog pl = new ParseLog(SD3Config.getAuditLog(), curTime, interval);

                if (DEBUG) System.out.println("About to read audit log");
                pl.readFile();
                if (DEBUG) System.out.println("Done reading audit log");

                if (SD3Config.getReplicateTargetPortionEnabled()) {
                    for (thold = 0.0; thold <= 100; thold += 1.0) {
                        ArrayList<String[]> records = pl.getFrequency(thold);
                        if (DEBUG) System.out.println("got " + records.size() + " records");

                        trimmed_records = trim(records);
                        if (DEBUG) System.out.println("got " + trimmed_records.size() + " trimmed records");
                        if (trimmed_records.size() / (double) SD3Config.getFilePerClusterCount() <= SD3Config.getReplicateTargetPortion() && chosen_threshold == 0) {
                            chosen_threshold = thold;
                            for (String[] item : trimmed_records) {
                                chosen_trimmed_records.add(item);
                            }
                            break;
                        }
                    }

                    if (DEBUG) System.out.println("chosen threshold = " + chosen_threshold + "\treplicating " + chosen_trimmed_records.size() + " files");
                } else {
                    thold = SD3Config.getReplicateThreshold();
                    ArrayList<String[]> records = pl.getFrequency(thold);
                    if (DEBUG) System.out.println("got " + records.size() + " records");

                    trimmed_records = trim(records);
                    if (DEBUG) System.out.println("got " + trimmed_records.size() + " trimmed records");
                    chosen_threshold = thold;
                    chosen_replica_percent = replica_percent;
                    for (String[] item : trimmed_records) {
                        chosen_trimmed_records.add(item);
                    }
                    if (DEBUG) System.out.println("chosen threshold = " + chosen_threshold + "\treplicating " + chosen_trimmed_records.size() + " files");
                }
            } else {
                ParseLog pl = new ParseLog(SD3Config.getAuditLog(), curTime, interval);

                pl.readFile();

                ArrayList<String[]> records = pl.getFrequency(0);
                trimmed_records = trim(records);
                for (String[] item : trimmed_records) {
                    chosen_trimmed_records.add(item);
                }
            }

            if (DEBUG) System.out.println("about to update");
            update(chosen_trimmed_records);
        } catch (Exception e) {
            System.err.println("Failure in audit log parsing thread:");
            e.printStackTrace();
        }
    }

    /*
     * Trim the records that are done by the local cluster. The local cluster doesn't replicate local files.
     */
    public ArrayList<String[]> trim(ArrayList<String[]> records) {
        ArrayList<String[]> result = new ArrayList<String[]>();
        for (String[] item : records) {
            if (SD3Config.ipToClusterNumberOrZero(item[0]) == SD3Config.getLocalCluster()) {

            } else {
                result.add(item);
            }
        }
        return result;
    }

    public void update(ArrayList<String[]> result) {

        if (DEBUG) System.out.println("replicate files start");
        String local_hdfs = SD3Config.getHdfsRootFor(SD3Config.getLocalCluster());
        for (String[] res : result) {
            int remoteClusterNumber = SD3Config.ipToClusterNumberOrZero(res[0]);

            if (remoteClusterNumber == 0) {
                if (DEBUG) System.out.println("Unknown cluster for " + res[0]);
                continue;
            }

            String local_uri = local_hdfs + res[1];
            String remote_hdfs = SD3Config.getHdfsRootFor(remoteClusterNumber);
            String remote_uri = remote_hdfs + res[1];

            try {
                Configuration conf = SD3Config.getHadoopConfig();
                FileSystem local_fs = FileSystem.get(URI.create(local_uri), conf);
                FileSystem remote_fs = FileSystem.get(URI.create(remote_uri), conf);
                FileUtil.copy(local_fs, new Path(local_uri), remote_fs, new Path(remote_uri), false, conf);
                cluster.addToTotalCopied(local_fs.getFileStatus(new Path(local_uri)).getLen());
                //System.out.println("Send file copy "+res[1]+ " from " + "local "+this.cluster.ip + " to "+cluster_name);
                synchronized (cluster.local_file) {
                    if (cluster.local_file.containsKey(res[1])) {
                        String clusters = cluster.local_file.get(res[1]);
                        clusters += "," + remoteClusterNumber;
                        cluster.local_file.put(res[1], clusters);
                    } else {
                        cluster.local_file.put(res[1], "" + remoteClusterNumber);
                    }
                }
                InetSocketAddress server = SD3Config.getListenerForCluster(remoteClusterNumber);
                String response = Helper.sendRequest(server, "COPY_" + res[1]);
                //System.compareStrings(string path, char* dirName)out.println("response from "+cluster_name.split(":")[0]+" is "+response);
            } catch (IOException ex) {
                System.err.println("replication: error copying " + local_uri + " to " + remote_uri);
                ex.printStackTrace();
            }
        }
    }

}
