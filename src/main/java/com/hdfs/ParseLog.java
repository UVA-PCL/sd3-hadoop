package com.hdfs;

/**
 * Created by Wyatt on 4/17/18.
 */

import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.concurrent.TimeUnit;

public class ParseLog{
    public ArrayList<LogEntry> records;
    private String filename;
    private Date curTime;
    private int interval;

    ParseLog(String file, Date curTime, int interval){
        this.records = new ArrayList<>();
        this.filename = file;
        this.curTime = curTime;
        this.interval = interval;
    }

    public void readFile(){
    	try {
            BufferedReader br = new BufferedReader(new FileReader(filename));
            String line = br.readLine();
            while (line != null) {
                String[] temp = line.split("\t| ");
                
                /*
                for(int i=0;i<temp.length;i++) {
                	System.out.println(temp[i]);
                }
                */
                
                /*
                Entry 0 is: 2019-03-09
				Entry 1 is: 15:41:16,643
				Entry 2 is: INFO
				Entry 3 is: FSNamesystem.audit:
				Entry 4 is: allowed=true
				Entry 5 is: ugi=zl4dc
				Entry 6 is: (auth:SIMPLE)
				Entry 7 is: ip=/127.0.0.1
				Entry 8 is: cmd=getfileinfo
				Entry 9 is: src=/
				Entry 10 is: dst=null
				Entry 11 is: perm=null
				Entry 12 is: proto=rpc
                */
                
                //LogEntry(String time, String ip, Cmd c, String path)
                Date oldTime  = curTime;
                
                try {
                    oldTime = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss").parse(line.substring(0,19));
                }
                
                catch (ParseException PE){
                    System.out.println("Parse Error!");
                }

                if(true){
                	//line = br.readLine();
                	LogEntry newLE = new LogEntry(temp[0] + " " + temp[1], temp[7].substring(4), Cmd.valueOf(temp[8].substring(4)), temp[9].substring(4));
                    records.add(newLE);
                    //line = br.readLine();
                    //System.out.println("add to log");

                }
                	
                else{
                	//System.out.println("Log entry added.");
                	LogEntry newLE = new LogEntry(temp[0] + " " + temp[1], temp[7].substring(4), Cmd.valueOf(temp[8].substring(4)), temp[9].substring(4));
                    records.add(newLE);
                    //line = br.readLine();
                }
                line = br.readLine();
                //System.out.println("Press Any Key To Continue...");
                //new java.util.Scanner(System.in).nextLine();
            }
        }
    	
    	
        catch(FileNotFoundException ex) {
            System.out.println(
                    "Unable to open file '" +
                            filename + "'");
        }
        catch(IOException ex) {
            System.out.println(
                    "Error reading file '"
                            + filename + "'");
        }
    }
    

    public void printAll(){
        for(LogEntry le : records){
        	System.out.println("Hello");
            System.out.println(le);
        }
    }

    public long getDateDiff(Date date1, Date date2, TimeUnit timeUnit) {
        long diffInMillies = date2.getTime() - date1.getTime();
        return timeUnit.convert(diffInMillies,timeUnit);
    }

    public ArrayList<String[]> getFrequency(double threshold){
        ArrayList<String[]> result = new ArrayList<>();
        HashMap<String, Integer> frequency = new HashMap<>();
        for(LogEntry le : records){
        	
            String key = le.getIp() + " "+ le.getSrc();
            if(frequency.containsKey(key)) {            	
            	if(le.getCmd().equals(Cmd.open))
            		frequency.put(key, frequency.get(key) + 1);
            	else if(le.getCmd().equals(Cmd.create))
            		frequency.put(key, frequency.get(key) - 1);
            }
            else {
            	if(le.getCmd().equals(Cmd.open))
            		frequency.put(key,1);
            	else if(le.getCmd().equals(Cmd.create))
            		frequency.put(key,-1);
            }
            
        }
        for(String k : frequency.keySet()){
            if(frequency.get(k) >= threshold){
                result.add(k.split(" "));
            } 
        }
        return result;
    }


    static class LogEntry{
        private String cmdTime;
        private String ipAddress;
        private Cmd cmd;
        private String src;

        LogEntry(String time, String ip, Cmd c, String path){
            cmdTime = time;
            ipAddress = ip;
            cmd = c;
            src = path;
        }

        public String getTime(){
            return cmdTime;
        }

        public String getIp(){
            return ipAddress;
        }

        public Cmd getCmd(){
            return cmd;
        }

        public String getSrc(){
            return src;
        }

        public String toString(){
            return cmdTime + " " + ipAddress + " " + cmd + " " + src;
        }

    }

    enum Cmd{
        open, create, delete, rename, mkdirs, listStatus, setReplication, setOwner, setPermission, getfileinfo
    }
}