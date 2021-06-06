package DSPPCode.mapreduce.kmeans_runner.question;

import DSPPCode.mapreduce.kmeans_runner.impl.KMeansRunnerImpl;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

public abstract class KMeansRunner extends Configured implements Tool {

  // 标识是否是最后一次迭代，true表示为最后一次迭代
  public static boolean isLastIteration = false;

  // 从0开始记录当前迭代步数
  public static int iteration = 0;

  // 配置项中用于记录当前迭代步数的键
  public static final String ITERATION = "1";

  @Override
  public final int run(String[] args) throws Exception {

    getConf().setInt(KMeansRunner.ITERATION, iteration);
    Job job = Job.getInstance(getConf(), getClass().getSimpleName());
    // 设置程序的类名
    job.setJarByClass(getClass());

    // 设置数据的输入输出路径
    FileInputFormat.addInputPath(job, new Path(args[0]));
    FileOutputFormat.setOutputPath(job, new Path(args[1] + iteration));

    // 设置map方法及其输出键值对数据类型
    job.setMapperClass(KMeansMapper.class);
    job.setMapOutputKeyClass(Text.class);
    job.setMapOutputValueClass(Text.class);

    // 将聚类中心集通过分布式缓存广播出去
    if (iteration == 0) {
      // 第一次迭代时的聚类中心集
      job.addCacheFile(new Path(args[2]).toUri());
    } else {
      // 广播上一次迭代输出的聚类中心集
      job.addCacheFile(new Path(args[1] + (iteration - 1)).toUri());
    }

    // 最后一次迭代输出的是聚类结果，不需要再计算新的聚类中心
    if (!isLastIteration) {
      job.setReducerClass(KMeansReducer.class);
      job.setOutputKeyClass(Text.class);
      job.setOutputValueClass(NullWritable.class);
    } else {
      job.setNumReduceTasks(0);
    }

    return job.waitForCompletion(true) ? 0 : 1;
  }

  /**
   * 用于执行K均值聚类的迭代过程
   *
   * @param args 输入参数为：数据集的输入路径 聚类结果输出路径 初始聚类中心集的输入路径 最大迭代次数
   */
  public final int executeIteration(String[] args) throws Exception {
    int exitCode = 0;
    int maxIteration = Integer.parseInt(args[3]);
    // 如果不是最后一次迭代则执行迭代计算
    while (!(isLastIteration = isLastIteration(args, iteration))) {
      exitCode = ToolRunner.run(new KMeansRunnerImpl(), args);
      if (exitCode == -1) {
        break;
      }
      iteration++;
    }

    if (maxIteration != 0) {
      // 执行最后一次迭代，以输出结果
      exitCode = ToolRunner.run(new KMeansRunnerImpl(), args);
    }
    return exitCode;
  }

  /**
   * 判断是否是最后一次迭代
   * @param args 输入参数，格式与{@link #executeIteration}的输入参数的格式一致
   * @param currentIteration 当前迭代步数
   * @return true表示为最后一次迭代，否则不是
   */
  protected abstract boolean isLastIteration(String[] args, int currentIteration);

}