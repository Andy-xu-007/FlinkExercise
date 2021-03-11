//package learning1
//
//import org.apache.flink.api.common.functions.RichReduceFunction
//import org.apache.flink.contrib.streaming.state.RocksDBStateBackend
//import org.apache.flink.streaming.api.TimeCharacteristic
//import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor
//import org.apache.flink.streaming.api.scala._
//import org.apache.flink.streaming.api.windowing.time.Time
//
//object window2Test {
//  def main(args: Array[String]): Unit = {
//    val env = StreamExecutionEnvironment.getExecutionEnvironment
//    env.setParallelism(1)
//    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)  // 设置时间语义，事件时间
//    env.getConfig.setAutoWatermarkInterval(500)  // 设置waterMark周期，默认是200
////    env.setStateBackend(new FsStateBackend("HDFS path"))  // 配置状态后端
//    env.setStateBackend(new RocksDBStateBackend(""))  // 需要引入pom包
//
//
//
////    val inputDataStream = env.readTextFile("E:\\workspace\\FlinkExercise\\src\\main\\resources\\seneor.txt")
//    val inputDataStream = env.socketTextStream("localhost", 777)
//
//    val dataStream = inputDataStream
//      .map(data => {
//        val arr = data.split(",")
//        SensorReading(arr(0), arr(1).toLong, arr(2).toDouble)
//      })
//      // BoundedOutOfOrdernessTimestampExtractor传入的参数是最大乱序程度，即延迟时间
//      .assignTimestampsAndWatermarks(new BoundedOutOfOrdernessTimestampExtractor[SensorReading](Time.seconds(3)) {
//        // 有界乱序数据提取器
//        override def extractTimestamp(element: SensorReading): Long = element.timetmp * 1000L
//      })  // 从当前数据中提取时间戳，并指定waterMark的生成方式，普遍
////      .assignAscendingTimestamps(_.timetmp * 1000L)  // 已经排好序的升序数据提取时间戳
//
//    val lateTag = new OutputTag[(String, Double, Long)]("late")
//    val resultStream = dataStream.map(data => (data.id, data.temperature, data.timetmp))
//      .keyBy(data => data._1) // 可以给0，表示第一个，只是目前是元组，按照二元组的第一个分组
//      //      .window(TumblingEventTimeWindows.of(Time.seconds(15)))  // 滚动时间窗口，15秒，还可以加便宜量，标准时区的偏移
//      // 滑动窗口，长度是15，滑动距离是3，还可以加上时区偏移
//      //      .window(SlidingProcessingTimeWindows.of(Time.seconds(15), Time.seconds(3)))
//      //      .window(EventTimeSessionWindows.withGap(Time.seconds(10)))  // 会话窗口，超过10就关闭，有新数据重新开启
//      //      .timeWindow(Time.seconds(15))  // 滚动窗口，底层调用的还是上面的，常用
//      //      .countWindow(10)  // 滚动技术窗口，再传一个参数，就是滑动窗口
//      // 每15秒统计一次，窗口内各传感器所有温度的最小值，以及最新的是时间戳
//      .timeWindow(Time.seconds(15))
////      .allowedLateness(Time.minutes(1))  // 处理迟到数据
//      .sideOutputLateData(lateTag)  // 侧输出流
//      //      .minBy(1)  // 第二个元素
//      //      .reduce((curRes, newData) => (curRes._1, curRes._2.min(newData._2), newData._3))
//      .reduce(new MyReducer)
//
////    resultStream.getSideOutput(lateTag).print("late")  // 获取侧输出
//    resultStream.print("result")
//
//
//    env.execute("window test")
//  }
//
//}
//
//// 自定义reduce
//class MyReducer extends RichReduceFunction[SensorReading] {
//  override def reduce(value1: SensorReading, value2: SensorReading): SensorReading = {
//    SensorReading(value1.id, value2.timetmp, value1.temperature.min(value2.temperature))
//  }
//}
//
//
