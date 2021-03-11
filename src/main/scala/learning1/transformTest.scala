//package learning1
//
//import org.apache.flink.api.common.functions.{MapFunction, ReduceFunction, RichMapFunction}
//import org.apache.flink.api.java.functions.KeySelector
//import org.apache.flink.api.scala.createTypeInformation
//import org.apache.flink.configuration.Configuration
//import org.apache.flink.streaming.api.scala.{ConnectedStreams, DataStream, StreamExecutionEnvironment}
//
///**
// * Copyright (c) 2020-2030 All right Reserved
// *
// * @author :   Andy Xu
// * @version :   1.0
// * @date :   8/16/2020 3:52 PM
// *
// */
//object transformTest {
//  def main(args: Array[String]): Unit = {
//    val env = StreamExecutionEnvironment.getExecutionEnvironment
//
//    val inputStreamFromFile = env.readTextFile("E:\\workspace\\FlinkExercise\\src\\main\\resources\\seneor.txt")
//
//    // 1，基本操作
//    val dataStream = inputStreamFromFile
//      .map(data => {
//        val dataArray = data.split(",")
//        SensorReading(dataArray(0), dataArray(1).toLong, dataArray(2).toDouble)
//      })
//
//    // 2，分组聚合操作
//    val aggStream = dataStream
//      //      .keyBy(0)
//      //      .keyBy("id")
//      //      .keyBy(data => data.id)
//      //      .keyBy()
//      .keyBy(new MyIDSelector())
//      //      .min("temperature")
//      //      .sum("temperature")
//      //      .reduce((curRes, newData) =>
//      //        learning1.SensorReading(curRes.id, curRes.timetmp.max(newData.timetmp), curRes.temperature.min(newData.temperature)))
//      .reduce(new MyReduce)
//
//
//    // 3，多流转换：分流
//    val splitStream: SplitStream[SensorReading] = dataStream
//      .split(data => {
//        if (data.temperature > 30)
//          Seq("high")
//        else
//          Seq("low")
//      })
//    val highTempStream: DataStream[SensorReading] = splitStream.select("high")
//    val lowStream = splitStream.select("low")
//    val allTempStream = splitStream.select("high", "low")
//
//    // 4， 合流
//    val warningStream: DataStream[(String, Double)] = highTempStream.map(
//      data => (data.id, data.temperature)
//    )
//    val connectedStreams: ConnectedStreams[(String, Double), SensorReading] = warningStream.connect(lowStream)
//    val resultStream: DataStream[Object] = connectedStreams.map(
//      warningData => (warningData._1, warningData._2, "high temp warning"),
//      lowTempData => (lowTempData.id, "normal")
//    )
//
//    // union合流两者类型必须一样，还可以合流三个
//    val unionStream = highTempStream.union(lowStream)
//
//
//
//    dataStream.print()
//    highTempStream.print("high")
//    env.execute("transform test job")
//  }
//
//}
//
//// 自定义函数类，key选择器
//class MyIDSelector() extends KeySelector[SensorReading, String] {
//  override def getKey(value: SensorReading): String = {
//    value.id
//  }
//}
//
//class MyReduce extends ReduceFunction[SensorReading]{
//  override def reduce(value1: SensorReading, value2: SensorReading): SensorReading =
//    SensorReading(value1.id, value1.timetmp.max(value2.timetmp), value1.temperature.min(value2.temperature))
//}
//
//// 自定义MapFunction
//class MyMapper extends MapFunction[SensorReading, (String, Double)] {
//  override def map(value: SensorReading): (String, Double) = (value.id, value.temperature)
//}
//
//class MyRichMapper extends RichMapFunction[SensorReading, Int] {
//
//  // open是创建RichFunction时候，初始化的操作
//  override def open(parameters: Configuration): Unit = super.open(parameters)
//
//  getRuntimeContext.getIndexOfThisSubtask
//
//  // map是每来一次，调用一次
//  override def map(value: SensorReading): Int = value.timetmp.toInt
//
//  // close 整个函数销毁的时候，调用的方法
//  override def close(): Unit = super.close()
//
//
//}
