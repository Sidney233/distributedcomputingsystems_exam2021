package DSPPCode.flink.twitter_hot_topics.impl;

import DSPPCode.flink.twitter_hot_topics.question.StopWordsOperation;
import DSPPCode.flink.twitter_hot_topics.question.TwitterHotTopics;
import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.util.Collector;
import java.io.IOException;
import java.util.List;

public class TwitterHotTopicsImpl extends TwitterHotTopics {

  @Override
  protected DataStream<Tuple2<String, Integer>> findHotTopics(DataStream<String> twitterText)
      throws IOException {
    List<String>  stopWordsList = StopWordsOperation.getStopWords();
    DataStream<Tuple2<String, Integer>> words = twitterText.flatMap(
        new FlatMapFunction<String, Tuple2<String, Integer>>() {
          @Override
          public void flatMap(String s, Collector<Tuple2<String, Integer>> collector)
              throws Exception {
            String[] word = s.split(" ");
            for (String w : word) {
              String regEx="[\n`~!@#$%^&*()+=\\-_|{}':;',\\[\\].<>/?~！@#￥%……&*（）——+|{}【】‘；：”“’。， 、？]";
              w = w.replaceAll( regEx , "");
              if (w.matches("^[a-zA-Z]*") && !stopWordsList.contains(w)) {
                w = w.toLowerCase();
                collector.collect(new Tuple2<>(w, 1));
              }
            }
          }
        });
    DataStream<Tuple2<String, Integer>> res = words.keyBy(0).reduce(
        new ReduceFunction<Tuple2<String, Integer>>() {
          @Override
          public Tuple2<String, Integer> reduce(Tuple2<String, Integer> stringIntegerTuple2,
              Tuple2<String, Integer> t1) throws Exception {
            return new Tuple2<>(stringIntegerTuple2.f0, stringIntegerTuple2.f1 + t1.f1);
          }
        })
        .filter(
        new FilterFunction<Tuple2<String, Integer>>() {
          @Override
          public boolean filter(Tuple2<String, Integer> stringIntegerTuple2) throws Exception {
            return stringIntegerTuple2.f1 >= HOT_TOPIC_THRESHOLD;
          }
        });
    res.print();
    return res;
  }
}
