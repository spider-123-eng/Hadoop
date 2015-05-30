
import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.hdfs.DistributedFileSystem;

public class TestConnection {
  public static void main(String[] args) throws IOException, URISyntaxException 
  {
    Configuration conf = new Configuration();
    FileSystem fileSystem = FileSystem.get(new URI("hdfs://localhost:8020"),conf);
    if(fileSystem instanceof DistributedFileSystem) {
      System.out.println("Successfully Connected to HDFS filesystem");
    }
    else {
      System.out.println("Other type of file system "+fileSystem.getClass());
    }
  }
}