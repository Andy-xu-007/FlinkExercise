//package learning1
//
//import org.apache.flink.api.common.serialization.SimpleStringSchema
//import org.apache.flink.streaming.api.scala.{DataStream, StreamExecutionEnvironment}
//import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer011
//
///**
// * Copyright (c) 2020-2030 All right Reserved
// *
// * @author :   Andy Xu
// * @version :   1.0
// * @date :   8/18/2020 9:16 PM
// *
// */
//object kafkaSink {
//  def main(args: Array[String]): Unit = {
//    val env = StreamExecutionEnvironment.getExecutionEnvironment
//    env.setParallelism(1) // 设置并行度
//
//    val inputStreamFromFile: DataStream[String] = env.readTextFile("E:\\workspace\\FlinkExercise\\src\\main\\resources\\seneor.txt")
//    val dataStream = inputStreamFromFile.map(data => {
//      val arr = data.split(",")
//      SensorReading(arr(0), arr(1).toLong, arr(2).toDouble).toString
//    })
//
//    dataStream.addSink(new FlinkKafkaProducer011[String]("localhost:9092", "my-topic", new SimpleStringSchema()))
//
//    // 官网
//    //    val myProducer = new FlinkKafkaProducer011[String](
//    //      "my-topic",                  // target topic
//    //      new SimpleStringSchema(),    // serialization schema
//    //      new Properties,                  // producer config
//    //      FlinkKafkaProducer011.Semantic.EXACTLY_ONCE) // fault-tolerance
//    //    dataStream.addSink(myProducer)
//
//    env.execute("kafka sink test")
//
//  }
//}
