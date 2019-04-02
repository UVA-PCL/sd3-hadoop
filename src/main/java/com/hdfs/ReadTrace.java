package com.hdfs;

//import com.hdfs.UpdateFiles;
import com.hdfs.runParseLog;
//import com.hdfs.ReadTrace.writeFile;

import java.io.*;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.URI;
//import java.net.URISyntaxException;
import java.text.SimpleDateFormat;
import java.util.*;
import java.util.concurrent.*;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.ipc.RemoteException;
//import org.apache.hadoop.fs.FileUtil;
import org.apache.hadoop.util.*;

//import javax.print.URIException;

public class ReadTrace {

	int total_bandwidth;
	
	final static SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");

	//final static String traceFile = "/home/zl4dc/tracedata/tracedata.txt";
	final static String root = System.getProperty("user.dir")+"/tracedata/";
	//final static String cluster1 = "128.143.63.229";
	//final static String cluster2 = "128.143.63.228";
	//final static String cluster3 = "128.143.137.232";

	static String cluster1 = "128.143.69.231:9000";//shen-18 file 10-19
	static String cluster2 = "128.143.69.230:9000";//shen-19 file 20-29
	static String cluster3 = "128.143.69.229:9000";//shen-20 file 30-39
	static String localhost = "";

	
	
	// fileSize += file.length();
	// fileSize+=1024;
	// fileSize /= 1048576;

	static class readFile implements Runnable {
		// String cluster;
		String file;
		long[] total_bandwidth;
		Vector<Long> localLatencies;
		Vector<Long> remoteLatencies;
		boolean local;
		

		readFile(String file, long[] bandwidth, Vector<Long> locLatencies, Vector<Long> remLatencies) {
			// this.cluster = cluster;
			
			this.file = file;
			this.total_bandwidth = bandwidth;
			this.localLatencies = locLatencies;
			this.remoteLatencies = remLatencies;
		}

		@Override
		public void run() {

			Configuration conf = new Configuration();

			conf.set("fs.hdfs.impl", org.apache.hadoop.hdfs.DistributedFileSystem.class.getName());
			conf.set("fs.file.impl", org.apache.hadoop.fs.LocalFileSystem.class.getName());	
			
			/*	
			try {
				
				String original_cluster = "";
				if (file.startsWith("file1"))
					original_cluster = cluster1;
				else if (file.startsWith("file2"))
					original_cluster = cluster2;
				else
					original_cluster = cluster3;
				if (!file.startsWith(cluster_number)){
					
					String uri = "hdfs://";
					uri= uri + cluster;
					uri = uri + "/file/data/";
					FileSystem hdfs = FileSystem.get(URI.create(uri), conf);
					String uri2 = uri + file + ".txt";
					System.out.println("WRITE: " +uri2);
					Path file_path = new Path(uri2);
					String origin_uri = "hdfs://"+original_cluster + "/file/data/" + file+".txt" ;
					File origin_file = new File(origin_uri);
					BufferedReader br = new BufferedReader(new FileReader(origin_file));
					OutputStream os = hdfs.create(file_path, new Progressable() {
												public void progress() {
							System.out.println("...bytes written");
						}
					});
					BufferedWriter bw = new BufferedWriter(new OutputStreamWriter(os, "UTF-8"));
					String line;
					while ((line = br.readLine()) != null) {
				        bw.write(line);
				        bw.newLine();
				    }
				}
			}
			*/
			
			String uri = "hdfs://" + localhost + "/file/data/" + file + ".txt";
			InputStream in = null;
			long startTime = System.currentTimeMillis();

			try {
				// FileSystem fs = FileSystem.get(URI.create(uri), conf);
				
				FileSystem fs = FileSystem.get(URI.create(uri), conf);
                in = fs.open(new Path(uri));
                
				byte[] file_buffer = new byte[in.available()];
				in.read(file_buffer);
				
				
				total_bandwidth[0] += Long.valueOf(file_buffer.length);
				//System.out.println("Opened file " + file + " in localhost");
				if(file.substring(4).startsWith("1")) {
					//System.out.println("read originial file locally");
				}
				else {
					//System.out.println("read replica file locally");
				}
				//IOUtils.copyBytes(in, System.out, 4096, false);
				long newLat = readLocalFileLatency(startTime);
				localLatencies.add(newLat);
			} catch (IOException ex) {
				//ex.printStackTrace();
				//System.out.println("File " + file + " not found in local hdfs");

				// if file not found in local hdfs
				String original_cluster = "";
				if (file.startsWith("file1"))
					original_cluster = cluster1;
				else if (file.startsWith("file2"))
					original_cluster = cluster2;
				else
					original_cluster = cluster3;
				
				uri = "hdfs://" + original_cluster + "/file/data/" + file + ".txt";
				try {
					FileSystem fs = FileSystem.get(URI.create(uri), conf);
	                in = fs.open(new Path(uri));
					

					byte[] file_buffer = new byte[in.available()];
					in.read(file_buffer);
					total_bandwidth[1] += Long.valueOf(file_buffer.length);

					//System.out.println(localhost + " opened file " + file + " in " + original_cluster);
					long newLat = readRemoteFileLatency(startTime);
					//IOUtils.copyBytes(in, System.out, 4096, false);
					remoteLatencies.add(newLat);
					
				}

				catch (IOException ex1) {
					//ex1.printStackTrace();
					//System.out.println("File " + file + " not found in original hdfs");
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
		String cluster_ip;
		String file;
		String cluster_number;
		Cluster cluster;
		writeFile(Cluster cluster, String file, String cluster_number) {
			this.cluster = cluster;
			this.cluster_ip = cluster.ip;
			this.file = file;
			this.cluster_number = cluster_number;
		}

		@Override
		public void run() {
			
			
			Configuration conf = new Configuration();
			conf.set("fs.hdfs.impl", org.apache.hadoop.hdfs.DistributedFileSystem.class.getName());
			conf.set("fs.file.impl", org.apache.hadoop.fs.LocalFileSystem.class.getName());
			
				
			String original_cluster = "";
			if (file.startsWith("file1"))
				original_cluster = cluster1;
			else if (file.startsWith("file2"))
				original_cluster = cluster2;
			else
				original_cluster = cluster3;

			
					
			String uri = "hdfs://";
			uri= uri + cluster_ip;
			uri = uri + "/file/data/";
			try {
				FileSystem hdfs = FileSystem.get(URI.create(uri), conf);
			} catch (IOException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
			String uri2 = uri + file + ".txt";
			//System.out.println("WRITE: " +uri2);
				
			String origin_uri = "hdfs://"+original_cluster + "/file/data/" + file+".txt" ;
				
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
			if(original_cluster.equals(localhost)) {
				//situation 1:local original file		
				synchronized(cluster) {
				if(cluster.local_file.containsKey("/file/data/"+file+".txt")) {//local file has replica
					InetSocketAddress ori_cluster = Helper.createSocketAddress(localhost.split(":")[0]+":22222");
					//System.out.println(Helper.sendRequest(ori_cluster, "DELETECOPY_"+"/file/data/"+file+".txt"));
				}
				//write file
				String Uri = "hdfs://"+localhost+"/file/data/"+file+".txt";
				write_File(Uri);
				}
			}
			else {//not original cluster
				//check whether localhost has the replica file
				if(cluster.remote_file.containsKey("/file/data/"+file+".txt")) {//situation 2:local replica file						
					//notify the original cluster to delete all other replica files
					String original_cluster_addr = cluster.remote_file.get("/file/data/"+file+".txt");
					InetSocketAddress ori_cluster = Helper.createSocketAddress(original_cluster_addr.split(":")[0]+":22222");
					//System.out.println(Helper.sendRequest(ori_cluster, "DELETECOPY_"+"/file/data/"+file+".txt"));
					//write the original file remotely
					
				}
				else {//situation 3:localhost does NOT have replica or original file
					//notify the original cluster to delete all other replica files
					InetSocketAddress ori_cluster = Helper.createSocketAddress(original_cluster.split(":")[0]+":22222");
					//System.out.println(Helper.sendRequest(ori_cluster, "DELETECOPY_"+"/file/data/"+file+".txt"));
					
					//write the original file remotely
						
				}
				String Uri = "hdfs://"+original_cluster+"/file/data/"+file+".txt";
				write_File(Uri);
			}
			
		}
	}
	public static void write_File(String uri) {
		//System.out.println("write file "+uri);
		Configuration conf = new Configuration();
		conf.set("fs.hdfs.impl", org.apache.hadoop.hdfs.DistributedFileSystem.class.getName());
		conf.set("fs.file.impl", org.apache.hadoop.fs.LocalFileSystem.class.getName());
		try {
			
			Path newPath = new Path(uri);

			FileSystem tempfs = FileSystem.get(URI.create(uri), conf);
			InputStream in = null;
			FileSystem fs = FileSystem.get(URI.create(uri), conf);
			in = fs.open(new Path(uri));
			
			//byte[] file_buffer = new byte[in.available()];
			
			//in.read(file_buffer);
			
			OutputStream out = tempfs.create(new Path(uri));
			IOUtils.copyBytes(in, out, 1024,true);
			tempfs.close();
		} catch (RemoteException re) {
			// TODO Auto-generated catch block
			System.out.println("Collision of file write happens. Request failed.");
		}
		catch(IOException ioe) {
			ioe.printStackTrace();
		}
	}
	
	public static void operate_Trace(String[] args,Cluster cluster) throws InterruptedException {
		if(args.length != 2) {
			System.out.println("argument number error");
			System.exit(0);
		}
		
		if(args[0].equals("1")) {
			localhost = cluster1;
		}
		else if(args[0].equals("2")) {
			localhost = cluster2;
		}
		else {
			localhost = cluster3;
		}
		Vector<Long> localLatencies = new Vector();
		Vector<Long> remoteLatencies = new Vector();
		long[] total_bandwidth = { 0, 0 };
		// total_bandwidth[0] is local, [1] is remote

		PrintWriter writer = null;
		try {
			writer = new PrintWriter(runParseLog.filename);
			writer.print("");
		} catch (FileNotFoundException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
			//System.out.println("Audit log file not found");
		} finally {
			writer.close();
		}

		long startTime = System.nanoTime();
		ExecutorService pool = Executors.newFixedThreadPool(1);

		try {
			BufferedReader br = new BufferedReader(new FileReader(root+args[1]));
			String line = br.readLine();
			int line_num=1;
			while (line != null) {
				String[] trace = line.split(" ");
				//System.out.println("read the trace line "+line_num+":"+line);
				line_num++;
				
				if(trace[1].equals("1") && localhost.equals(cluster1)){
					if (trace[2].equals("write")) {
						pool.execute(new writeFile(cluster, "file"+trace[4], trace[1]) );
					}
					
					else{
						pool.execute(new readFile("file"+trace[4], total_bandwidth, localLatencies, remoteLatencies));
					}
                }
				
                if(trace[1].equals("2") && localhost.equals(cluster2)){
                	if (trace[2].equals("write")) {
						pool.execute(new writeFile(cluster, "file"+trace[4], trace[1]) );
					}
					
					else{
						pool.execute(new readFile("file"+trace[4], total_bandwidth, localLatencies, remoteLatencies));
					}
                }
                
                if(trace[1].equals("3") && localhost.equals(cluster3)){
                	if (trace[2].equals("write")) {
						pool.execute(new writeFile(cluster, "file"+trace[4], trace[1]) );
					}
					
					else{
						pool.execute(new readFile("file"+trace[4], total_bandwidth, localLatencies, remoteLatencies));
					}
                }

				//pool.execute(new readFile("file"+trace[4], total_bandwidth, localLatencies, remoteLatencies));

				line = br.readLine();
			}

			br.close();
		}

		catch (FileNotFoundException ex) {
			ex.printStackTrace();
			//System.out.println("Unable to open file '" + traceFile + "'");
		} catch (IOException ex) {
			ex.printStackTrace();
			//System.out.println("Error reading file '" + traceFile + "'");
		}

		pool.shutdown();
		// while (!pool.isTerminated()) {
		// System.out.println("Wrong with terminating threads");
		// }
		pool.awaitTermination(Long.MAX_VALUE, TimeUnit.MINUTES);

		System.out.println("Finished all threads");
		long endTime = System.nanoTime();
		long totalTime = endTime - startTime;
		System.out.println((double) totalTime / 1000000000.0+"s");
		
		double avg_local_latency = 0.0;
		double avg_remote_latency = 0.0;
		int abandoned = 0;
		for (int i = 0; i < localLatencies.size(); i++) {
			if(localLatencies.elementAt(i)<50)
				avg_local_latency += localLatencies.elementAt(i);
			else
				abandoned++;
		}
		System.out.println("total abandoned local latency = "+abandoned);
		
		avg_local_latency = avg_local_latency / localLatencies.size();
		abandoned = 0;
		for (int i = 0; i < remoteLatencies.size(); i++) {
			if(remoteLatencies.elementAt(i)<50)
				avg_remote_latency += remoteLatencies.elementAt(i);
			else
				abandoned++;
			//System.out.println(remoteLatencies.elementAt(i));
		}
		avg_remote_latency = avg_remote_latency / remoteLatencies.size();
		System.out.println("total abandoned remote latency = "+abandoned);
		
		System.out.println("number of local requests = "+localLatencies.size());
		System.out.println("total number of requests on this cluster = "+(localLatencies.size()+remoteLatencies.size()));
		System.out.println("percentage of local requests on this cluster = "+localLatencies.size()/(localLatencies.size()+remoteLatencies.size()));
		
		System.out.println("average local latency for reads: "+avg_local_latency);
		
		System.out.println("average remote latency for reads: "+avg_remote_latency);
		
		System.out.println("local bandwidth usage for reads: ");
		System.out.println(total_bandwidth[0]);
		
		System.out.println("remote bandwidth usage for reads: ");
		System.out.println(total_bandwidth[1]);
	}
	

	public static void main(String[] args) throws InterruptedException {
		
		if(args.length != 5) {
			System.out.println("error: number of arguments, arg[0] should be cluster number, arg[1] should be trace file name");
			System.out.println("args[2] to args[4] is cluster IP address");
			System.exit(0);
		}
		int i=1;
		Cluster cluster;
		cluster1 = args[2];
		cluster2 = args[3];
		cluster3 = args[4];
		if(args[0].equals("1")) {
			cluster = new Cluster(cluster1);
			localhost = cluster1;
		}
		else if(args[0].equals("2")) {
			cluster = new Cluster(cluster2);
			localhost = cluster2;
		}
		else {
			cluster = new Cluster(cluster3);
			localhost = cluster3;
		}	
		
		Listener listener = new Listener(cluster);
		listener.start();
		//while(true) {
		
		System.out.println("Execute trace without policy");
		operate_Trace(args,cluster);
		
		ScheduledExecutorService executor = Executors.newScheduledThreadPool(1);
		System.out.println("Execution finished. Begin replicating files under the policy.");	        
		executor.scheduleAtFixedRate(new runParseLog(Calendar.getInstance().getTime(), 0, args,cluster,true),
											0, 3600, TimeUnit.SECONDS);

		System.out.println("Replicating finished.");
		
		while(cluster.others__partial_replication_unfinish != 0) {
			Thread.sleep(300);
		}
		
		System.out.println("All finished. Begin executing the trace.");
		operate_Trace(args,cluster);
		
		System.out.println("Execution finished. Begin replicating all the files");
		
		executor.scheduleAtFixedRate(new runParseLog(Calendar.getInstance().getTime(), 0, args,cluster,false),
				0, 3600, TimeUnit.SECONDS);
		System.out.println("Replicating finished.");

		System.out.println("Send finish all replication message to others. Waiting.");
		while(cluster.others__all_replication_unfinish != 0) {
			Thread.sleep(300);
		}
		
		System.out.println("All finished. Begin executing the trace.");
		
		operate_Trace(args,cluster);
		
		System.out.println("Run over.");
		System.exit(0);
		//}
		
		

	}
}
