package DSPPTest.student.mapreduce.kmeans_runner;

import static DSPPTest.util.FileOperator.deleteFolder;
import static DSPPTest.util.FileOperator.readFile2String;
import static DSPPTest.util.Verifier.verifyKV;

import DSPPCode.mapreduce.kmeans_runner.impl.KMeansRunnerImpl;
import DSPPTest.student.TestTemplate;
import DSPPTest.util.Parser.KVParser;
import org.junit.Test;

public class KMeansRunnerTest extends TestTemplate {

  @Test
  public void test() throws Exception {
    // 设置路径
    String pointsInputPath = root + "/mapreduce/kmeans_runner/input/data";
    String centersInputPath = root + "/mapreduce/kmeans_runner/input/center/";
    String outputPath = outputRoot + "/mapreduce/kmeans_runner/";
    String answerFile = root + "/mapreduce/kmeans_runner/answer";

    // 删除旧输出
    deleteFolder(outputPath);

    // 执行
    KMeansRunnerImpl kMeansRunner = new KMeansRunnerImpl();
    String[] args = {pointsInputPath, outputPath, centersInputPath, "4"};
    int exitCode = kMeansRunner.executeIteration(args);

    String outputFile = outputPath + KMeansRunnerImpl.iteration + "/part-m-00000";
    // 检验结果
    verifyKV(readFile2String(outputFile), readFile2String(answerFile), new KVParser("\t"));

    System.out.println("恭喜通过~");
    System.exit(exitCode);
  }
}
