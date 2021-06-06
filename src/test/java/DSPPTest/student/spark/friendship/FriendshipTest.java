package DSPPTest.student.spark.friendship;

import DSPPCode.spark.friendship.impl.FriendshipImpl;
import DSPPTest.student.TestTemplate;
import DSPPTest.util.Parser.KVListParser;
import org.junit.Test;

import static DSPPTest.util.FileOperator.deleteFolder;
import static DSPPTest.util.FileOperator.readFile2String;
import static DSPPTest.util.Verifier.verifyKV;

public class FriendshipTest extends TestTemplate {

  @Test
  public void test() throws Exception {
    // 设置路径
    String inputPath = root + "/spark/friendship/input";
    String outputPath = outputRoot + "/spark/friendship";
    String outputFile = outputPath + "/part-00000";
    String answerFile = root + "/spark/friendship/answer";

    // 删除旧输出
    deleteFolder(outputPath);

    // 执行
    String[] args = {inputPath, outputPath};
    FriendshipImpl friendshipImpl = new FriendshipImpl();
    friendshipImpl.run(args);

    // 检验结果
    verifyKV(readFile2String(outputFile), readFile2String(answerFile), new KVListParser());

    System.out.println("恭喜通过~");
  }
}

