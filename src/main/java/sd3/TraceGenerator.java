package sd3;

import java.io.BufferedWriter;
import java.io.File;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Random;
import java.util.Scanner;

public class TraceGenerator {
    public static void main(String[] args) throws Exception {
        SD3Config.setClusterIPsFromProperties();

        Scanner reader = new Scanner(System.in);

        int clustNum = SD3Config.getMaxClusterNumber();

        System.out.print("Enter number of relative reads: "); System.out.flush();
        int readNum = Integer.parseInt(reader.nextLine());

        System.out.print("Enter number of relative writes: "); System.out.flush();
        int writeNum = Integer.parseInt(reader.nextLine());

        System.out.print("Enter trace record count: "); System.out.flush();
        int lines = Integer.parseInt(reader.nextLine());

        System.out.print("Output file name: "); System.out.flush();
        String fileName = reader.nextLine();

        reader.close();
        Path filePath = Paths.get(SD3Config.getTraceDataRoot(), fileName);

        try (BufferedWriter bw = Files.newBufferedWriter(filePath)) {

            for (int i = 0; i < lines; ++i) {
                Random ran = new Random();
                int cluster = ran.nextInt(clustNum) + 1;
                int home_cluster = ran.nextInt(clustNum) + 1;
                int fileTemp = ran.nextInt(100);
                int fileCount = SD3Config.getFilePerClusterCount();
                int fileN = 0;
                if (fileTemp <= 24) {
                    fileN = ran.nextInt(fileCount / 100);
                } else if (fileTemp <= 43 && 24 < fileTemp) {
                    fileN = ran.nextInt(fileCount / 10);
                } else if (fileTemp <= 56 && 43 < fileTemp) {
                    fileN = ran.nextInt(fileCount / 10 * 2);
                } else if (fileTemp <= 66 && 56 < fileTemp) {
                    fileN = ran.nextInt(fileCount / 10 * 3);
                } else if (fileTemp <= 74 && 66 < fileTemp) {
                    fileN = ran.nextInt(fileCount / 10 * 4);
                } else if (fileTemp <= 81 && 74 < fileTemp) {
                    fileN = ran.nextInt(fileCount / 10 * 5);
                } else if (fileTemp <= 87 && 81 < fileTemp) {
                    fileN = ran.nextInt(fileCount / 10 * 6);
                } else if (fileTemp <= 92 && 87 < fileTemp) {
                    fileN = ran.nextInt(fileCount / 10 * 7);
                } else if (fileTemp <= 96 && 92 < fileTemp) {
                    fileN = ran.nextInt(fileCount / 10 * 8);
                } else {
                    fileN = ran.nextInt(fileCount);
                }

                int file = home_cluster * 1000000 + fileN;

                int tempOpt = ran.nextInt(readNum + writeNum);
                int optnum = 0;

                if (tempOpt < readNum) {
                    optnum = 0;
                } else {
                    optnum = 1;
                }

                String[] opt = {"read", "write"};

                String line = "Cluster " + cluster + " " + opt[optnum] + " file " + file + "\n";
                bw.write(line);
            }
            bw.close();
            System.out.println("Trace generated in " + filePath);
        } catch (java.io.IOException ex) {
            ex.printStackTrace();
        }
    }
}
