package DSPPCode.spark.friendship.impl;

import DSPPCode.spark.friendship.question.Friendship;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFlatMapFunction;
import org.apache.spark.api.java.function.PairFunction;
import scala.Tuple2;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Iterator;
import java.util.List;

public class FriendshipImpl extends Friendship {

  /**
   * TODO 请完成该方法
   * <p>
   * 请在此方法中完成求解共同好友的功能
   *
   * @param lines
   */
  @Override
  public JavaPairRDD<String, Iterable<String>> findFriendShip(JavaRDD<String> lines) {
    JavaPairRDD<String, Iterable<String>> table = lines.mapToPair(
        new PairFunction<String, String, Iterable<String>>() {
          @Override
          public Tuple2<String, Iterable<String>> call(String s) throws Exception {
            String[] data = s.split(" ");
            String[] fri = Arrays.copyOfRange(data, 1, data.length);
            Arrays.sort(fri);
            Iterable<String> friends = orderedPairs(fri);
            return new Tuple2<String, Iterable<String>>(data[0], friends);
          }
        });

    JavaPairRDD<String, String> pair = table.flatMapToPair(
        new PairFlatMapFunction<Tuple2<String, Iterable<String>>, String, String>() {
          @Override
          public Iterator<Tuple2<String, String>> call(
              Tuple2<String, Iterable<String>> stringIterableTuple2) throws Exception {
            List<Tuple2<String, String>> res = new ArrayList<>();
            for (String p : stringIterableTuple2._2) {
              res.add(new Tuple2<>(p, stringIterableTuple2._1));
            }
            return res.iterator();
          }
        });

    JavaPairRDD<String, Iterable<String>> res = pair.groupByKey();
    System.out.println(res.take(100));
    return res;
  }

  /**
   * TODO 请完成该方法
   * <p>
   *
   * @param strs 无重复字符串的、已按字典序升序排序的有序字符串数组（数组中的每个字符串中仅包含一个字母）
   * @return 将数组中的字符串按升序字典序两两连接（任意两个字符串之间仅需连接一次），以形成一个新的字符串，
   * 返回一个由所有新的字符串所组成的列表，列表中新的字符串之间的顺序任意。
   * 例如，给定字符串数组 ["A", "B", "C"]，则应当返回一个列表 {"AB", "AC", "BC"}，
   * 其中"AB"、"AC"和"BC"之间的顺序任意。
   */
  @Override
  public List<String> orderedPairs(String[] strs) {
    List<String> res = new ArrayList<>();
    for (int i = 0; i < strs.length - 1; i++) {
      for (int j = i + 1; j < strs.length; j++) {
        res.add(strs[i] + strs[j]);
      }
    }
    return res;
  }
}
