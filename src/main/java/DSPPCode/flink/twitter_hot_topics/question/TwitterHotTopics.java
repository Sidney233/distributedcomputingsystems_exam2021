package DSPPCode.flink.twitter_hot_topics.question;

import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import DSPPCode.flink.twitter_hot_topics.impl.TwitterTextHandlerImpl;

/**
 * @author ikroal
 */
public abstract class TwitterHotTopics {

  /**
   * 热点话题的阈值，话题的个数超过或者等于该阈值时为热点话题
   */
  protected static final int HOT_TOPIC_THRESHOLD = 4;


  public int run(String[] args) throws Exception {
    StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

    // 从文本中读取数据
    DataStream<String> streamSource = env.readTextFile(args[0]);
    // 对Twitter文本做处理
    DataStream<String> twitterText = streamSource.map(new TwitterTextHandlerImpl());
    // 统计文本中的热点话题
    DataStream<Tuple2<String, Integer>> hotTopics = findHotTopics(twitterText);

    // 将结果写入文件
    hotTopics.writeAsText(args[1]);

    // 设置停词表的路径
    StopWordsOperation.stopWordsPath = args[2];
    env.execute(getClass().getName());

    return 0;
  }

  protected abstract DataStream<Tuple2<String, Integer>> findHotTopics(
      DataStream<String> twitterText);
}
