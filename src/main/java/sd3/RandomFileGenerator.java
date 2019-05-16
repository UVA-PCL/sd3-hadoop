package sd3;

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
        Path fullPath = new File(SD3Config.getGeneratedFileRoot(), fileName).toPath();
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

    private static void generateForCluster(int n) throws IOException {
        int file_n = SD3Config.getFilePerClusterCount();
        for (int i = 0; i <= (file_n - 1); i++) {
            int index = 1000000 * n + i;
            new RandomFileGenerator("file" + index + ".txt", 100000).generate();
        }
    }

    public static void main(String[] args) throws IOException {
        int for_cluster = args.length == 0 ? -1 : Integer.parseInt(args[0]);
        SD3Config.setClusterIPsFromProperties();
        int cluster_n = SD3Config.getMaxClusterNumber();

        if (for_cluster == -1) {
            for (int n = 1; n <= cluster_n; n++) {
                generateForCluster(n);
            }
        } else {
            generateForCluster(for_cluster);
        }
        System.out.println("Done generating files");
    }

}
