package sd3;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.ipc.RemoteException;

import java.io.*;
import java.net.InetSocketAddress;
import java.net.URI;
import java.text.DateFormat;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.Date;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

public class ReadTrace {
    private static final boolean DEBUG = false;

    private static String now() {
        return DateFormat.getDateTimeInstance(DateFormat.LONG, DateFormat.LONG).format(new Date());
    }

    int total_bandwidth;

    static class readFile implements Runnable {
        // String cluster;
        String file;
        long[] total_bandwidth;
        ArrayList<Long> localLatencies;
        ArrayList<Long> remoteLatencies;


        readFile(String file, long[] bandwidth, ArrayList<Long> locLatencies, ArrayList<Long> remLatencies) {
            this.file = file;
            this.total_bandwidth = bandwidth;
            this.localLatencies = locLatencies;
            this.remoteLatencies = remLatencies;
        }

        @Override
        public void run() {
            if (DEBUG) System.out.println("read " + file);

            Configuration conf = SD3Config.getHadoopConfig();

            String uri = SD3Config.getLocalPathFor(file);
            InputStream in = null;
            long startTime = System.currentTimeMillis();

            try {
                FileSystem fs = FileSystem.get(URI.create(uri), conf);
                Path p = new Path(uri);
                FileStatus status = fs.getFileStatus(p);
                int length = (int) status.getLen();

                in = fs.open(new Path(uri));

                byte[] file_buffer = new byte[length];
                int offset = 0;
                int count = 0;
                while (count < length) {
                    int new_count = in.read(file_buffer, offset, length - offset);
                    count += new_count;
                    if (new_count <= 0) break;
                }

                total_bandwidth[0] += file_buffer.length;
                //IOUtils.copyBytes(in, System.out, 4096, false);
                long newLat = readLocalFileLatency(startTime);
                localLatencies.add(newLat);
            } catch (IOException ex) {
                //ex.printStackTrace();
                if (DEBUG) System.out.println("File " + file + " not found in local hdfs");

                // if file not found in local hdfs
                uri = SD3Config.getHomePathFor(file);
                try {
                    FileSystem fs = FileSystem.get(URI.create(uri), conf);
                    Path p = new Path(uri);
                    in = fs.open(p);
                    FileStatus status = fs.getFileStatus(p);
                    int length = (int) status.getLen();


                    byte[] file_buffer = new byte[length];
                    int count = 0;
                    int offset = 0;
                    while (count < length) {
                        int new_count = in.read(file_buffer, offset, length - offset);
                        count += new_count;
                        if (new_count <= 0) break;
                    }
                    total_bandwidth[1] += Long.valueOf(file_buffer.length);

                    //System.out.println(localhost + " opened file " + file + " in " + original_cluster);
                    long newLat = readRemoteFileLatency(startTime);
                    //IOUtils.copyBytes(in, System.out, 4096, false);
                    remoteLatencies.add(newLat);

                } catch (IOException ex1) {
                    System.err.println("File " + file + " not accessible in original hdfs:");
                    ex1.printStackTrace();
                    //System.out.println(ex1);
                }

            } finally {
                IOUtils.closeStream(in);
            }

        }

        private long readLocalFileLatency(long startTime) {
            long endTime = System.currentTimeMillis();
            long newLat = endTime - startTime;
            return newLat;

        }

        private long readRemoteFileLatency(long startTime) {
            long endTime = System.currentTimeMillis();
            long newLat = endTime - startTime;
            return newLat;
        }
    }

    static class writeFile implements Runnable {
        String file;
        final Cluster cluster;

        writeFile(Cluster cluster, String file) {
            this.cluster = cluster;
            this.file = file;
        }

        @Override
        public void run() {


            Configuration conf = SD3Config.getHadoopConfig();

            String uri = SD3Config.getLocalPathFor(file);
            String origin_uri = SD3Config.getHomePathFor(file);

            /*situations
             * 1.local cluster has the file to be written, the file is the original file
             *
             * solution: delete all other replica files, then write file
             *
             *
             * 2.local cluster has the file to be written, the file is the replica file
             *
             * solution: notify the original cluster to delete all other replica files,
             * 			 then write the original file remotely
             *
             *
             * 3.local cluster does NOT have the file to be written
             *
             * solution: notify the original cluster to delete all other replica files,
             * 			 then write the original file remotely
             *
             */
            if (uri.equals(origin_uri)) {
                //situation 1:local original file
                synchronized (cluster) {
                    if (cluster.local_file.containsKey(SD3Config.getLocalPathFor(file))) {//local file has replica
                        InetSocketAddress ori_cluster = SD3Config.getListenerForCluster(SD3Config.getLocalCluster());
                        Helper.sendRequest(ori_cluster, "DELETECOPY_" + "/file/data/" + file + ".txt");
                    }
                    //write file
                    write_File(uri);
                }
            } else {//not original cluster
                //check whether localhost has the replica file
                if (cluster.remote_file.containsKey(SD3Config.getLocalPathFor(file))) {//situation 2:local replica file
                    //notify the original cluster to delete all other replica files
                    InetSocketAddress ori_cluster = SD3Config.getListenerForCluster(SD3Config.homeIdForFile(file));
                    Helper.sendRequest(ori_cluster, "DELETECOPY_" + "/file/data/" + file + ".txt");
                    //write the original file remotely

                } else {//situation 3:localhost does NOT have replica or original file
                    //notify the original cluster to delete all other replica files
                    InetSocketAddress ori_cluster = SD3Config.getListenerForCluster(SD3Config.homeIdForFile(file));
                    Helper.sendRequest(ori_cluster, "DELETECOPY_" + "/file/data/" + file + ".txt");

                    //write the original file remotely

                }
                write_File(origin_uri);
            }

        }
    }

    private static void detectWriteCollision(String file) {
        if (DEBUG)
            System.out.println("Writing " + file + ": write collision detected.");
    }

    public static void write_File(String uri) {
        if (DEBUG) System.out.println("write file " + uri);
        Configuration conf = SD3Config.getHadoopConfig();
        try {

            FileSystem tempfs = FileSystem.get(URI.create(uri), conf);
            InputStream in = null;
            FileSystem fs = FileSystem.get(URI.create(uri), conf);
            in = fs.open(new Path(uri));

            //byte[] file_buffer = new byte[in.available()];

            //in.read(file_buffer);

            OutputStream out = tempfs.create(new Path(uri));
            IOUtils.copyBytes(in, out, 1024, true);
        } catch (RemoteException re) {
            detectWriteCollision(uri);
        } catch (FileNotFoundException fne) {
            if (fne.getMessage().contains("does not have any open files")) {
                detectWriteCollision(uri);
            } else {
                System.err.println("Could not write file " + uri);
                fne.printStackTrace();
            }
        } catch (IOException ioe) {
            System.err.println("Could not write file " + uri);
            ioe.printStackTrace();
        }
    }

    private static void clearAuditLog() {
        PrintWriter writer = null;
        try {
            writer = new PrintWriter(SD3Config.getAuditLog());
            writer.print("");
        } catch (FileNotFoundException e) {
            System.err.println("Warning: Error clearing audit log");
            e.printStackTrace();
        } finally {
            writer.close();
        }
    }

    public static void operate_Trace(String traceFile, Cluster cluster) throws InterruptedException, IOException {
        ArrayList<Long> localLatencies = new ArrayList<Long>();
        ArrayList<Long> remoteLatencies = new ArrayList<Long>();
        long[] total_bandwidth = {0, 0};
        // total_bandwidth[0] is local, [1] is remote


        long startTime = System.nanoTime();
        ExecutorService pool = Executors.newFixedThreadPool(1);

        BufferedReader br = new BufferedReader(new FileReader(SD3Config.getTraceDataRoot() + "/" + traceFile));
        String line = br.readLine();
        int line_num = 1;
        while (line != null) {
            String[] trace = line.split(" ");
            //System.out.println("read the trace line "+line_num+":"+line);
            line_num++;
            if (Integer.parseInt(trace[1]) == SD3Config.getLocalCluster()) {
                if (trace[2].equals("write")) {
                    pool.execute(new writeFile(cluster, "file" + trace[4]));
                } else {
                    pool.execute(new readFile("file" + trace[4], total_bandwidth, localLatencies, remoteLatencies));
                }
            }
            line = br.readLine();
        }

        br.close();


        pool.shutdown();
        // while (!pool.isTerminated()) {
        // System.out.println("Wrong with terminating threads");
        // }
        pool.awaitTermination(Long.MAX_VALUE, TimeUnit.MINUTES);

        System.out.println("Finished all threads");
        long endTime = System.nanoTime();
        long totalTime = endTime - startTime;
        System.out.println((double) totalTime / 1000000000.0 + "s");

        double avg_local_latency = 0.0;
        double avg_remote_latency = 0.0;
        int abandoned = 0;
        for (int i = 0; i < localLatencies.size(); i++) {
            if (localLatencies.get(i) < 50)
                avg_local_latency += localLatencies.get(i);
            else
                abandoned++;
        }
        System.out.println("total abandoned local latency = " + abandoned);

        avg_local_latency = avg_local_latency / localLatencies.size();
        abandoned = 0;
        for (int i = 0; i < remoteLatencies.size(); i++) {
            if (remoteLatencies.get(i) < 50)
                avg_remote_latency += remoteLatencies.get(i);
            else
                abandoned++;
            //System.out.println(remoteLatencies.elementAt(i));
        }
        avg_remote_latency = avg_remote_latency / remoteLatencies.size();
        System.out.println("total abandoned remote latency = " + abandoned);

        System.out.println("number of local requests = " + localLatencies.size());
        System.out.println("total number of requests on this cluster = " + (localLatencies.size() + remoteLatencies.size()));
        System.out.println("percentage of local requests on this cluster = " + localLatencies.size() / (localLatencies.size() + remoteLatencies.size()));

        System.out.println("average local latency for reads: " + avg_local_latency);

        System.out.println("average remote latency for reads: " + avg_remote_latency);

        System.out.println("local bandwidth usage for reads: ");
        System.out.println(total_bandwidth[0]);

        System.out.println("remote bandwidth usage for reads: ");
        System.out.println(total_bandwidth[1]);
    }

    public static void runExperiment(String traceFile, Cluster cluster, boolean cleanFirst, boolean replicate, boolean replicateWithPolicy) throws InterruptedException, IOException {
        System.out.println("At " + now() + ": Run trace " + traceFile + " " +
                (replicate ?
                        (replicateWithPolicy ? "with replication policy" : "with unfiltered replication") :
                        "without replication"
                )
                +
                (cleanFirst ? "" : " (without erasing previously replicated data)")
        );
        if (cleanFirst) {
            DeleteExtra.clearExtraReplicasOnCluster(SD3Config.getLocalCluster());
            clearAuditLog(); // FIXME: previously deliberately(?) done, but possibly not before scheduleAtFixedRate?
        }
        ScheduledExecutorService executor = Executors.newScheduledThreadPool(1);
        if (replicate) {
            executor.scheduleAtFixedRate(
                    new RunParseLog(Calendar.getInstance().getTime(), SD3Config.getReplicateHistoryInterval(), cluster, replicateWithPolicy),
                    0, SD3Config.getReplicateInterval(), TimeUnit.SECONDS);
        }
        operate_Trace(traceFile, cluster);
        for (InetSocketAddress listener : SD3Config.getRemoteListeners()) {
            Helper.sendRequest(listener, "FINISH," + SD3Config.getLocalCluster());
        }
        System.out.println("Waiting for other clusters to finish");
        cluster.waitForRemotes();
        System.out.println("Done waiting");
        if (replicate) {
            executor.shutdown();
        }
    }


    public static void main(String[] args) throws Exception {
        if (args.length != 5) {
            System.out.println("error: number of arguments, arg[0] should be cluster number, arg[1] should be trace file name");
            System.out.println("and: ");
            System.out.println("   the System property sd3.cluster-hosts should be set the main host names for all clusters");
            System.out.println("   the System proprety sd3.tracedata to the location of the trace to read from");
            System.out.println("   the System proprety sd3.audit-log to the location of the HDFS audit log");
            System.exit(0);
        }
        String traceFile = args[1];
        SD3Config.setClusterIPsFromProperties();
        SD3Config.setLocalCluster(Integer.parseInt(args[0]));
        Cluster cluster;
        cluster = new Cluster(SD3Config.getLocalClusterIP());

        System.out.println("Starting at " + now());
        Listener listener = new Listener(cluster);
        listener.start();

		/* test the trace with
		      no replication,
		      replication according to the threshold
		      replication of everything

		   in each case, replication is done "live", by periodically scanning the audit log and making
		   new decisions about replication
		 */
        System.out.println("************* Running baseline: no replication ***************\n");
        runExperiment(traceFile, cluster, true, false, false);
        Thread.sleep(30 * 1000);
        System.out.println("************* Running replication with policy ***************\n");
        runExperiment(traceFile, cluster, true, true, true);
        Thread.sleep(30 * 1000);
        System.out.println("************* Running replication of everything accessed ***************\n");
        runExperiment(traceFile, cluster, true, true, false);
        Thread.sleep(30 * 1000);
        System.out.println("************* Accessing everything after replicating previously ***************\n");
        runExperiment(traceFile, cluster, false, true, false);
        System.out.println("************* About to stop main thread ****************\n");

        listener.toDie();

        System.exit(0);
    }
}
