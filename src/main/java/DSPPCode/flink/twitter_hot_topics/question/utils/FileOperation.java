package DSPPCode.flink.twitter_hot_topics.question.utils;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.net.URI;

public class FileOperation {

  /**
   * 用于标识传入路径为 HDFS 路径
   */
  private static final String HDFS = "hdfs";

  /**
   * 从指定路径获取输入流
   *
   * @param path 本地路径或 HDFS 路径
   */
  public static InputStream read(String path) {
    boolean isHDFS = path.contains(HDFS);
    InputStream inputStream = null;
    try {
      if (isHDFS) {
        FileSystem fs = getFileSystem(path);
        inputStream = fs.open(new Path(path));
      } else {
        inputStream = new FileInputStream(new File(path));
      }
    } catch (IOException e) {
      e.printStackTrace();
    }
    return inputStream;
  }

  /**
   * @param path HDFS 文件系统的目标路径，此路径决定将要使用的文件系统，如果没有指定则会使用默认的文件系统
   * @return 返回 FileSystem 实例，用于访问文件系统
   */
  public static FileSystem getFileSystem(String path) {
    FileSystem fs = null;
    try {
      fs = FileSystem.get(URI.create(path), new Configuration());
    } catch (IOException e) {
      e.printStackTrace();
    }
    return fs;
  }
}
