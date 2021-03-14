package learning2.state

import learning2.SensorReading
import org.apache.flink.api.common.functions.RichFlatMapFunction
import org.apache.flink.api.common.state.{ValueState, ValueStateDescriptor}
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.streaming.api.scala._
import org.apache.flink.util.Collector
/**
 * 需求： 对传感器温度值跳变，超过10度就报警
 */
object StateExercise {
  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    // 如果并行度不设置为1，那么后面writeAsCsv将会写入到多个文件，out.csv将会是文件夹
    env.setParallelism(1)

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

    val altStream = dataStream
      .keyBy(_.id)
//      .flatMap(new TempChangeAlter(10.0))
      .flatMapWithState[(String, Double, Double), Double]{
        case (data: SensorReading, None) => ( List.empty, Some(data.temperature))
        case (data: SensorReading, lastTemp: Some[Double]) => {
          // 跟最新的温度值求差值
          val diff = (data.temperature - lastTemp.get).abs
          if (diff > 10.0)
            (List((data.id,lastTemp.get, data.temperature)), Some(data.temperature))
          else
            (List.empty, Some(data.temperature))
        }
      }
    altStream.print()

    env.execute("state test")
  }

}

// 实现自定义RichFlatMapFunction
class TempChangeAlter(threshold: Double) extends RichFlatMapFunction[SensorReading, (String, Double, Double)] {
  // 定义状态，保存上一次的温度值
  lazy val lastTempState: ValueState[Double] = getRuntimeContext
    .getState(new ValueStateDescriptor[Double]("lasttemp", classOf[Double]))

  override def flatMap(value: SensorReading, out: Collector[(String, Double, Double)]): Unit = {
    // 获取上次的温度
    val lastTemp = lastTempState.value()
    // 跟最新的温度值求差值
    val diff = (value.temperature - lastTemp).abs
    if (diff > threshold)
      out.collect(value.id, lastTemp, value.temperature)

    // 更新状态
    lastTempState.update(value.temperature)

  }
}
