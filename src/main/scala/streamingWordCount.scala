import org.apache.flink.api.java.utils.ParameterTool
import org.apache.flink.api.scala.createTypeInformation
import org.apache.flink.streaming.api.scala.{DataStream, StreamExecutionEnvironment}

/**
 * Copyright (c) 2020-2030 All right Reserved
 *
 * @author :   Andy Xu
 * @version :   1.0
 * @date :   8/11/2020 9:46 PM
 *
 */

// 流处理
object streamingWordCount {
  def main(args: Array[String]): Unit = {
    // 创建流处理
    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    // 配置并行度,默认是核心的数量
    env.setParallelism(8)
    env.disableOperatorChaining()  // 默认所有任务都打散

    // 从程序运行参数中读取hostname和port，也可以当作参数传进来
    val params = ParameterTool.fromArgs(args)
    val hostname = params.get("host")
    val port = params.getInt("port")
    // 接收socker文本流
    val inputDataStream: DataStream[String] = env.socketTextStream("node2", 7777)

    // 定义转换操作，word  Count
    val resultDataStream: DataStream[(String, Int)] = inputDataStream
      .flatMap(_.split(" ")).disableChaining()  // 前后one to one放在一个solt里的任务，强行切断，
      .map((_, 1)).setParallelism(4)
      .keyBy(0).startNewChain() // 按照第一个元素分组，后面的算子是和前面的断开，和接下来的合并一个新的chain
//      .sum(1)  // 按照第二个元素聚合

    resultDataStream.print().setParallelism(1)  // 等待下面代码的执行，不同于批处理
    // setParallelism(1)，并行度为1，则控制台输出的时候就没有了前面的序号
    env.execute("stream word count job")  // 启动执行，括号内是任务的名称


  }
}
