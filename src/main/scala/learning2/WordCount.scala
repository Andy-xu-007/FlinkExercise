package learning2

import org.apache.flink.api.scala.{DataSet, ExecutionEnvironment}
import org.apache.flink.api.scala._

object WordCount {
  def main(args: Array[String]): Unit = {
    // 创建一个批处理执行环境
    val env: ExecutionEnvironment = ExecutionEnvironment.getExecutionEnvironment

    // 从文件中读取数据
    val inputPath = "E:\\workSpace\\FlinkExercise\\src\\main\\resources\\word.txt"
    val inputDataSet: DataSet[String] = env.readTextFile(inputPath)

    // 对数据进行转换处理统计，先分词，再按照word进行分组，最后统计输出
    val resultDataSet = inputDataSet
      .flatMap(_.split(" "))
      .map((_,1))
      .groupBy(0)  // 根据元组中某个位置的key来分组，从0开始
      .sum(1)  // 对所有数据的第二个元素求和

    resultDataSet.print()
  }

}
