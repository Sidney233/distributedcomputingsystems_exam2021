package DSPPCode.flink.twitter_hot_topics.question;

import java.io.IOException;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.ObjectMapper;

/**
 * 对推特文本进行处理
 * @author ikroal
 */
public abstract class TwitterTextHandler extends RichMapFunction<String, String> {

  /**
   * 用于将json格式的数据映射为Java对象
   */
  private transient ObjectMapper jsonToTwitter;

  @Override
  public void open(Configuration parameters) throws Exception {
    super.open(parameters);
    jsonToTwitter = new ObjectMapper();
  }

  /**
   * 将每条Json格式的Twitter数据映射为Twitter对象
   * @param twitterJson twitter数据
   * @return {@link Twitter} 对象
   */
  protected Twitter fromJson(String twitterJson) throws IOException {
    return jsonToTwitter.readValue(twitterJson, Twitter.class);
  }

  /**
   * 对推特文本进行处理
   * @param twitterJson Json格式的推特数据
   * @return 处理后的推特文本
   */
  @Override
  public abstract String map(String twitterJson) throws Exception;
}
