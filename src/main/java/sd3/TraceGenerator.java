package sd3;

import java.io.BufferedWriter;
import java.io.File;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Random;
import java.util.Scanner;

public class TraceGenerator {
    private static String root = SD3Config.getTraceDataRoot();

    public static void main(String[] args) throws Exception {
        Scanner reader = new Scanner(System.in);

        System.out.println("Enter number of clusters: ");
        int clustNum = reader.nextInt();

        System.out.println("Enter number of relative reads: ");
        int readNum = reader.nextInt();

        System.out.println("Enter number of relative writes: ");
        int writeNum = reader.nextInt();

        System.out.println("Trace record count: ");
        int lines = reader.nextInt();

        reader.close();

        String fileName = "trace" + lines + ".txt";
        Path fullPath = new File(root, fileName).toPath();

        try (BufferedWriter bw = Files.newBufferedWriter(fullPath)) {

            for (int i = 0; i < lines; ++i) {
                Random ran = new Random();
                int cluster = ran.nextInt(clustNum) + 1;
                int home_cluster = ran.nextInt(clustNum) + 1;
                int fileTemp = ran.nextInt(100);
                int fileN = 0;
                if (fileTemp <= 24) {
                    fileN = ran.nextInt(10);
                } else if (fileTemp <= 43 && 24 < fileTemp) {
                    fileN = ran.nextInt(100);
                } else if (fileTemp <= 56 && 43 < fileTemp) {
                    fileN = ran.nextInt(200);
                } else if (fileTemp <= 66 && 56 < fileTemp) {
                    fileN = ran.nextInt(300);
                } else if (fileTemp <= 74 && 66 < fileTemp) {
                    fileN = ran.nextInt(400);
                } else if (fileTemp <= 81 && 74 < fileTemp) {
                    fileN = ran.nextInt(500);
                } else if (fileTemp <= 87 && 81 < fileTemp) {
                    fileN = ran.nextInt(600);
                } else if (fileTemp <= 92 && 87 < fileTemp) {
                    fileN = ran.nextInt(700);
                } else if (fileTemp <= 96 && 92 < fileTemp) {
                    fileN = ran.nextInt(800);
                } else {
                    fileN = ran.nextInt(900);
                }

                int file = home_cluster * 1000 + fileN;

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
            System.out.println("Trace generated in " + fileName);
        } catch (java.io.IOException ex) {
            ex.printStackTrace();
        }
    }
}
