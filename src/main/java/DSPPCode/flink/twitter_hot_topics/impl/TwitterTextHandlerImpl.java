package DSPPCode.flink.twitter_hot_topics.impl;

import DSPPCode.flink.twitter_hot_topics.question.Twitter;
import DSPPCode.flink.twitter_hot_topics.question.TwitterTextHandler;

public class TwitterTextHandlerImpl extends TwitterTextHandler {

  /**
   * 对推特文本进行处理
   *
   * @param twitterJson Json格式的推特数据
   * @return 处理后的推特文本
   */
  @Override
  public String map(String twitterJson) throws Exception {
    Twitter tw = fromJson(twitterJson);
    if (tw.getUser().getLang().equals("en")) {
      return tw.getText();
    } else {
      return "";
    }
  }
}
