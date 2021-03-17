package learning2.flinksql

import learning2.SensorReading
import org.apache.flink.api.scala.ExecutionEnvironment
import org.apache.flink.streaming.api.scala._
import org.apache.flink.table.api.{EnvironmentSettings, TableEnvironment}
import org.apache.flink.table.api.bridge.scala.{BatchTableEnvironment, StreamTableEnvironment}

object TableApi {
  def main(args: Array[String]): Unit = {
    // 1，创建表的执行环境
    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    // 如果并行度不设置为1，那么后面writeAsCsv将会写入到多个文件，out.csv将会是文件夹
    env.setParallelism(1)

    // 1.1 基于老版本planner的流处理
    val settingOld = EnvironmentSettings.newInstance()
      .useOldPlanner()
      .inStreamingMode()
      .build()
    val oldStreamTableEnv = StreamTableEnvironment.create(env, settingOld)

    // 1.2 基于老版本的批处理
    val batchEnv = ExecutionEnvironment.getExecutionEnvironment
    val oldBatchTableEnv = BatchTableEnvironment.create(batchEnv)

    // 1.3 基于blink planner 的流处理
    val bsStreamSettings = EnvironmentSettings.newInstance()
      .useBlinkPlanner()
      .inStreamingMode()
      .build()
    val bsStreamTableEnv = StreamTableEnvironment.create(env, bsStreamSettings)

    // 1.4 基于blink planner 的批处理
    val bsBatchSettings = EnvironmentSettings.newInstance()
      .useBlinkPlanner()
      .inBatchMode()
      .build()
    val bsBatchTableEnv = TableEnvironment.create(bsBatchSettings)




    val inputPath = "E:\\workSpace\\FlinkExercise\\src\\main\\resources\\seneor.txt"

    val inputStream = env.readTextFile(inputPath)

    // 先转换成样例类
    val dataStream = inputStream.map(
      data => {
        val arr = data.split(",")
        SensorReading(arr(0), arr(1).toLong, arr(2).toDouble)
      }
    )


    env.execute("table api test")
  }

}
