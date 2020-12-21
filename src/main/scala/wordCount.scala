import org.apache.flink.api.scala.{AggregateDataSet, DataSet, ExecutionEnvironment, createTypeInformation}

/**
 * Copyright (c) 2020-2030 All right Reserved
 *
 * @author :   Andy Xu
 * @version :   1.0
 * @date :   8/11/2020 9:18 PM
 *
 */

// 批处理
object wordCount {
  def main(args: Array[String]): Unit = {
    // 创建一个批处理的执行环境
    val env: ExecutionEnvironment = ExecutionEnvironment.getExecutionEnvironment
    // 从文件中读取数据
    val inputDataSet: DataSet[String] = env.readTextFile("E:\\workspace\\FlinkExercise\\src\\main\\resources\\word.txt")
    // /基于DataSet做转换,
    val resultDataSet: AggregateDataSet[(String, Int)] = inputDataSet
      .flatMap(_.split(" "))
      .map((_, 1))
      .groupBy(0) // 0指的是以二元组中第一个元素作为key分组
      .sum(1)  // 聚合二元组中第二个元素的值

    resultDataSet.print()

  }
}
