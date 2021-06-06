package DSPPCode.flink.twitter_hot_topics.question;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.List;
import DSPPCode.flink.twitter_hot_topics.question.utils.FileOperation;

public class StopWordsOperation {

  /**
   * 停词表
   */
  private static List<String> stopWords = new ArrayList<>();
  /**
   * 停词表的路径
   */
  public static String stopWordsPath;

  /**
   * 获取停词表
   */
  public static List<String> getStopWords() throws IOException {
    if (stopWordsPath == null) {
      throw new IllegalStateException("stopWordsPath is null");
    }
    // 仅停词表为空时才执行加载，避免重复加载
    if (stopWords.size() == 0) {
      BufferedReader bufferedReader = new BufferedReader(new InputStreamReader(
          FileOperation.read(stopWordsPath)));
      String stopWord;
      while ((stopWord = bufferedReader.readLine()) != null) {
        stopWords.add(stopWord);
      }
    }
    return stopWords;
  }
}
