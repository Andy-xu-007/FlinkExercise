//package learning1
//
//import org.apache.flink.api.common.serialization.SimpleStringSchema
//import org.apache.flink.api.scala.createTypeInformation
//import org.apache.flink.streaming.api.functions.source.SourceFunction
//import org.apache.flink.streaming.api.scala.{DataStream, StreamExecutionEnvironment}
//
//import java.util.Properties
//import scala.collection.immutable
//import scala.util.Random
//
///**
// * Copyright (c) 2020-2030 All right Reserved
// *
// * @author :   Andy Xu
// * @version :   1.0
// * @date :   8/13/2020 9:38 PM
// *
// */
//
//case class SensorReading(id: String, timetmp: Long, temperature: Double)
//object sourceTest {
//  def main(args: Array[String]): Unit = {
//    val env = StreamExecutionEnvironment.getExecutionEnvironment
//    env.setParallelism(1)
//
//    // 1, 从集合中读取数据
//    val stream1: DataStream[SensorReading] = env.fromCollection(List(
//      SensorReading("sensor_1", 154684269, 35.8),
//      SensorReading("Sensor_6", 158468655, 15.6),
//      SensorReading("sensor_7", 154684356, 31.8),
//      SensorReading("Sensor_10", 158468369, 25.6)
//    ))
//
//    // 2, 从文件中读取
//    val stream2: DataStream[String] = env.readTextFile("E:\\workspace\\FlinkExercise\\src\\main\\resources\\seneor.txt")
//
//    // 3, socket 文本流
//
//    // 4， kafka
//    val props = new Properties()
//    props.setProperty("bootstrap.servers", "localhost:9092")
//    props.setProperty("group.id", "consumer-group")
//    props.setProperty("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer")
//    props.setProperty("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer")
//    props.setProperty("auto.offset.reset", "latest")
//    val stream4 = env.addSource(new FlinkKafkaConsumer011[String]("my-topic", new SimpleStringSchema(), props))
//
//    // 5, 直接传数据，测试的时候用
//    val stream5 = env.fromElements(0, 1.1, "andy")
//
//    // 6， 自定义source
//
//    val stream6 = env.addSource(new MySensorSource())
//
//    //  输出
//    stream1.print("stream1")
//    env.execute("source test job")
//
//  }
//}
//
//
//// 自定义一个sourceFunction，自动生成测试数据
//class MySensorSource() extends SourceFunction[SensorReading] {
//  // 定义一个flag，来表示数据原是否正常运行
//  var running: Boolean = true
//
//  // 随机生成SensorReading数据
//  override def run(sourceContext: SourceFunction.SourceContext[SensorReading]): Unit = {
//    // 定义一个随机数发生器
//    val rand = new Random()
//
//    // 随机生成10个传感器的温度值，并且不停在之前温度基础上更新（随机上下波动）
//    // 首先生成10个传感器的初始温度, 高斯分布
//    var curTemps: immutable.Seq[(String, Double)] = 1.to(10).map(
//      i => ("sensor_" + i, 60 + rand.nextGaussian())
//    )
//
//    while (running) {
//      // 在当前温度基础上，随机生成微小波动
//      curTemps = curTemps.map(
//        data => (data._1, data._2 + rand.nextGaussian())
//      )
//
//      // 获取当前系统时间
//      val curTs = System.currentTimeMillis()
//      // 包装成样例类，用ctx发送数据
//      curTemps.foreach(
//        data => sourceContext.collect(SensorReading(data._1, curTs, data._2))
//      )
//
//      // 定义间隔时间
//      Thread.sleep(1000L)
//    }
//
//  }
//
//  override def cancel(): Unit = running = false
//}
