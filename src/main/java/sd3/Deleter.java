package sd3;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FileUtil;
import org.apache.hadoop.fs.Path;

import java.io.IOException;
import java.net.URI;

public class Deleter extends Thread {
    private static final boolean DEBUG = false;

    String file_path;

    public Deleter(String path) {
        file_path = path;
    }

    @SuppressWarnings("deprecation")
    @Override
    public void run() {
        Configuration conf = SD3Config.getHadoopConfig();
        FileSystem fs = null;
        try {
            fs = FileSystem.get(URI.create(file_path), conf);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }

        Path hdfs = new Path(file_path);
        try {
            FileUtil.fullyDelete(fs, hdfs);

        } catch (IOException e) {
            throw new RuntimeException(e);
        }

        if (DEBUG) System.out.println("delete file " + file_path);

    }
}
