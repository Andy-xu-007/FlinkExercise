package learning2

import org.apache.flink.api.common.functions.ReduceFunction
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.windowing.assigners.{EventTimeSessionWindows, SlidingProcessingTimeWindows, TumblingEventTimeWindows}
import org.apache.flink.streaming.api.windowing.time.Time

object WindowTest {
  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
//    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)
    env.getConfig.setAutoWatermarkInterval(500)  // 设置自动给生成waterMark的周期


    val inputPath = "E:\\workSpace\\FlinkExercise\\src\\main\\resources\\seneor.txt"

    val inputStream = env.readTextFile(inputPath)

    // 先转换成样例类
    val dataStream = inputStream.map(
      data => {
        val arr = data.split(",")
        SensorReading(arr(0), arr(1).toLong, arr(2).toDouble)
      }
    )
//      .assignAscendingTimestamps(_.timeStamp * 1000)  // 升序数据提取时间戳
      .assignTimestampsAndWatermarks(new BoundedOutOfOrdernessTimestampExtractor[SensorReading](Time.seconds(3)) {
        override def extractTimestamp(element: SensorReading): Long = element.timeStamp * 1000L
      })  // 最大乱序程度是三秒


    val lateTag = new OutputTag[(String, Double, Long)]("late")
    // 每15秒统计一次，窗口内各传感器所有温度的最小值，以及最新的时间戳
    val resultStream: DataStream[(String, Double, Long)] = dataStream
      .map(data => (data.id, data.temperature, data.timeStamp))
      .keyBy(_._1) // 按照二元组的第一个（id）分组
            .window(TumblingEventTimeWindows.of(Time.seconds(15)))  // 滚动时间窗口
//            .window(SlidingProcessingTimeWindows.of(Time.seconds(15), Time.seconds(3)))  // 滑动窗口
      //      .window(EventTimeSessionWindows.withGap(Time.seconds(10)))  // 会话窗口
//      .timeWindow(Time.seconds(15)) // 时间滚动窗口的一种简写形式
      //      .countWindow(10)  // 计数滚动窗口
      //      .minBy(1)
      .allowedLateness(Time.minutes(1))  // 允许迟到数据
      .sideOutputLateData(lateTag)  // 最后的数据放到侧输出流里
      .reduce((curRes, newData) => (curRes._1, curRes._2.min(newData._2), newData._3))

      resultStream.getSideOutput(lateTag).print("late")
      resultStream.print()

    env.execute("window test")
  }

}

class MyReducer extends ReduceFunction[SensorReading]{
  override def reduce(value1: SensorReading, value2: SensorReading): SensorReading = {
    SensorReading(value1.id, value2.timeStamp, value1.temperature.min(value2.temperature))
  }
}
