package learning2

import org.apache.flink.api.common.functions.ReduceFunction
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.streaming.api.scala._

object TransformTest {
  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    val inputPath = "E:\\workSpace\\FlinkExercise\\src\\main\\resources\\seneor.txt"

    val inputStream = env.readTextFile(inputPath)

    // 1, 先转换成样例类
    val dataStream = inputStream.map(
      data => {
        val arr = data.split(",")
        SensorReading(arr(0), arr(1).toLong, arr(2).toDouble)
      }
    )

    // 2, 分组聚合，输出每个传感器当前最小值
    // keyBy/min 可以传入位置，也可以传入字段名
    val aggStream = dataStream.keyBy("id")
      .minBy("temperature")

//    aggStream.print()

    // 3.1, 输出当前最小温度值，以及最近的时间戳，reduce
    val resultStream = dataStream.keyBy("id")
      .reduce((curState, newData) =>
        SensorReading(curState.id, newData.timeStamp, curState.temperature.min(newData.temperature))
      )
//    resultStream.print()

    // 4，多流转换操作
    // 4.1 分流，将传感器温度数据分成低温，高温两条流, 已被侧输出流替代了

//    val splitStream = dataStream
//      .split(data => {
//        if (data.temperature > 30.0) Seq("high") else Seq("low")
//      })
//    val highTemStream = splitStream.select("high")
//    val lowTemStream = splitStream.select("low")
//    val allTempStream = splitStream.select("high", "low")

    env.execute("Transform Test")
  }

}

// 自定义ReduceFunction
class  MyReduceFunction extends ReduceFunction[SensorReading] {
  override def reduce(t: SensorReading, t1: SensorReading): SensorReading = {
    SensorReading(t.id, t1.timeStamp, t.temperature.min(t1.temperature))
  }
}
