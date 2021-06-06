package DSPPCode.flink.twitter_hot_topics.question;

import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.annotation.JsonIgnoreProperties;

/**
 * @author ikroal
 */
@JsonIgnoreProperties(ignoreUnknown = true)
public class Twitter {

  /**
   * 推特文本
   */
  private String text;

  /**
   * 发布文本的用户信息
   */
  private User user;

  @JsonIgnoreProperties(ignoreUnknown = true)
  public static class User {

    /**
     * 用户发布文本所使用的语言
     */
    private String lang;

    public String getLang() {
      return lang;
    }
  }

  public String getText() {
    return text;
  }

  public User getUser() {
    return user;
  }
}
