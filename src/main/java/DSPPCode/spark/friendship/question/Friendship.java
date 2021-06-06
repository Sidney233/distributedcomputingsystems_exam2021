package DSPPCode.spark.friendship.question;

import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import java.io.Serializable;
import java.util.List;

public abstract class Friendship implements Serializable {

  private static final String MODE = "local";

  public void run(String[] args) {
    JavaSparkContext sc = new JavaSparkContext(MODE, getClass().getName());
    // 读入文本数据，创建名为lines的RDD
    JavaRDD<String> lines = sc.textFile(args[0]);
    JavaPairRDD<String, Iterable<String>> friendship = findFriendShip(lines);
    // 输出结果到文本文件中
    friendship.saveAsTextFile(args[1]);
    sc.close();
  }

  /**
   * TODO 请完成该方法
   * <p>
   * 请在此方法中完成求解共同好友的功能
   */
  public abstract JavaPairRDD<String, Iterable<String>> findFriendShip(JavaRDD<String> lines);

  /**
   * TODO 请完成该方法
   * <p>
   * @param strs 无重复字符串的、已按字典序升序排序的有序字符串数组（数组中的每个字符串中仅包含一个字母）
   * @return 将数组中的字符串按升序字典序两两连接（任意两个字符串之间仅需连接一次），以形成一个新的字符串，
   * 返回一个由所有新的字符串所组成的列表，列表中新的字符串之间的顺序任意。
   * 例如，给定字符串数组 ["A", "B", "C"]，则应当返回一个列表 {"AB", "AC", "BC"}，
   * 其中"AB"、"AC"和"BC"之间的顺序任意。
   */
  public abstract List<String> orderedPairs(String[] strs);
}

