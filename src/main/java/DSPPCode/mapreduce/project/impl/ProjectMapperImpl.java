package DSPPCode.mapreduce.project.impl;

import DSPPCode.mapreduce.project.question.ProjectMapper;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import java.io.IOException;

public class ProjectMapperImpl extends ProjectMapper {

  /**
   * TODO 请完成该方法
   * <p>
   * 输入:
   * <p>
   * value中包含三个字段："序号"、"姓名"和"城市"，各字段之间按逗号分隔。
   * <p>
   * 例如，0,Alice,Shanghai 代表一条"序号"为0、"姓名"为Alice、"城市"为Shanghai的记录。
   *
   * @param key
   * @param value
   * @param context
   */
  @Override
  public void map(Object key, Text value, Context context)
      throws IOException, InterruptedException {
    String[] row = value.toString().split(",");
    Long index = Long.parseLong(row[0]) + 1;
    context.write(new Text(index.toString() + "," + row[1]), NullWritable.get());
  }
}
