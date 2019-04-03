package com.hdfs;

/**
 * Created by Wyatt on 4/17/18.
 */
import java.io.BufferedWriter;
import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Random;

import java.util.Scanner;



public class RandomFileGenerator {

    private final String fileName;
    private final int lines;

    RandomFileGenerator(String fileName, int lineCount) {
        this.fileName = fileName;
        this.lines = lineCount;
    }

    void generate() throws IOException {
        Path fullPath = new File(SD3Config.getTraceDataRoot(), fileName).toPath();
        // make sure file exists
        Files.createDirectories(fullPath.getParent());
        try (BufferedWriter bw = Files.newBufferedWriter(fullPath)) {
        	bw.write("VERSION:1\n");
            for (int i = 0; i < lines; ++i) {
                String line = generateRandomString(20);
                bw.write(line);
            }
        }
    }
    
    public String generateRandomString(int length) {
    	  
        int leftLimit = 97; // letter 'a'
        int rightLimit = 122; // letter 'z'
        int targetStringLength = length;
        Random random = new Random();
        StringBuilder buffer = new StringBuilder(targetStringLength);
        for (int i = 0; i < targetStringLength; i++) {
            int randomLimitedInt = leftLimit + (int) 
              (random.nextFloat() * (rightLimit - leftLimit + 1));
            buffer.append((char) randomLimitedInt);
        }
        String generatedString = buffer.toString();
	    
        return generatedString;
    }
    
    public static void main(String[] args) throws IOException {
        Random ran = new Random();
        
        //Scanner reader = new Scanner(System.in);  // Reading from System.in
        //System.out.println("Enter cluster number: ");
        //int n = reader.nextInt();
        for(int n = 1; n <= 3; n++) {
        	for(int i = 0;i <= 999;i++){
        		//int line = ran.nextInt(10000000-1000000+1) + 1000000;
        		int index = 1000*n + i;
        		new RandomFileGenerator("file" +index+".txt", 100000).generate();
        		System.out.println(i);
        	}
        }
    }

}
