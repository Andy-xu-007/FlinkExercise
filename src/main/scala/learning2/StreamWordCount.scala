package learning2

import org.apache.flink.api.java.utils.ParameterTool
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.api.scala._

object StreamWordCount {
  def main(args: Array[String]): Unit = {

    // 创建流处理的执行环境
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    // 设置并行度，不设置就等于核心数
    // 会根据key的hash值来分配并行度的序号
//    env.setParallelism(32)

    // 从外部命令中提取参数，作为socket主机名和端口号
    val paramTool = ParameterTool.fromArgs(args)
    val host = paramTool.get("host")
    val port = paramTool.getInt("port")

    // 接收一个socket文本流
//    val inputDataStream = env.socketTextStream("node01", 9999)
    val inputDataStream = env.socketTextStream(host, port)

    // 进行转换处理
    // 以下算子后面的附加算子都可以去掉
    val resultDataStream = inputDataStream
      .flatMap(_.split(" ")).slotSharingGroup("a")
      .filter(_.nonEmpty).disableChaining() // .disableChaining()表示前后禁止合并任务
      .map((_,1)).setParallelism(1)  // 可以手动为每个算子设置并行度，一般不会单独设置
      .keyBy(0)  // keyby代替groupby
      .sum(1).startNewChain()  // 开启一个新的任务链，和之前的任务链切分

    // setParallelism 可以用在最终输出放在一个文件里
    // 以下设置并行度，最终的输出行的开头将没有序号
    resultDataStream.print().setParallelism(1)

    // 启动任务执行，和普通的wordCount不一样，必须要启动一个进程来开启
    env.execute("stream word count")
  }
}
