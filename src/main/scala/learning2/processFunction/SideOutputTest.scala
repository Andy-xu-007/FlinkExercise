package learning2.processFunction

import learning2.SensorReading
import org.apache.flink.api.common.restartstrategy.RestartStrategies
import org.apache.flink.api.common.state.StateTtlConfig.RocksdbCompactFilterCleanupStrategy
import org.apache.flink.contrib.streaming.state.RocksDBStateBackend
import org.apache.flink.runtime.state.filesystem.FsStateBackend
import org.apache.flink.runtime.state.memory.MemoryStateBackend
import org.apache.flink.streaming.api.CheckpointingMode
import org.apache.flink.streaming.api.functions.ProcessFunction
import org.apache.flink.streaming.api.scala._
import org.apache.flink.util.Collector

object SideOutputTest {
  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    // 如果并行度不设置为1，那么后面writeAsCsv将会写入到多个文件，out.csv将会是文件夹
    env.setParallelism(1)
//    env.setStateBackend(new MemoryStateBackend())  // 配置状态后端级别
//    env.setStateBackend(new FsStateBackend(""))  // 状态后端级别，和上面的类似
//    env.setStateBackend(new RocksDBStateBackend(""))  // 类上
    // 可以配置异步check point
//    env.enableCheckpointing(1000L) // source触发时间间隔
//    env.getCheckpointConfig.setCheckpointingMode(CheckpointingMode.EXACTLY_ONCE)
//    env.getCheckpointConfig.setCheckpointTimeout(60000L)
//    env.getCheckpointConfig.setMaxConcurrentCheckpoints(2)  // 最多允许5个checkPoint同时在处理
//    env.getCheckpointConfig.setMinPauseBetweenCheckpoints(500L)  // 两个check point之间间隔时间
//    env.getCheckpointConfig.setPreferCheckpointForRecovery(true) //
//    env.getCheckpointConfig.setTolerableCheckpointFailureNumber(3)  // 容忍多少次check point失败
//
//    // check point 重启策略
//    env.setRestartStrategy(RestartStrategies.fixedDelayRestart(3,10000L)) // 重启三次，间隔10秒


    val inputPath = "E:\\workSpace\\FlinkExercise\\src\\main\\resources\\seneor.txt"

    //    val inputStream = env.readTextFile(inputPath)
    val inputStream = env.socketTextStream("node01", 9999)

    // 先转换成样例类
    val dataStream = inputStream.map(
      data => {
        val arr = data.split(",")
        SensorReading(arr(0), arr(1).toLong, arr(2).toDouble)
      }
    )

    val highTempStream = dataStream
      .process(new SplitTempProcessor(30.0))

    highTempStream.print("high")
    highTempStream.getSideOutput(new OutputTag[(String, Long, Double)]("low")).print("low")

    env.execute("side output test")
  }

}

// 实现自定义的ProcessTempFunction, 进行分流
class SplitTempProcessor(threshold: Double) extends ProcessFunction[SensorReading, SensorReading] {
  override def processElement(value: SensorReading, ctx: ProcessFunction[SensorReading,
    SensorReading]#Context, out: Collector[SensorReading]): Unit = {
    // 如果当前温度值大于30， 那么输出到主流
    if (value.temperature > threshold) {
      out.collect(value)
    } else {
      // 不大于30， 则输出到侧输出流
      ctx.output(new OutputTag[(String, Long, Double)]("low"), (value.id, value.timeStamp, value.temperature))
    }
  }
}
