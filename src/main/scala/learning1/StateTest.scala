//package learning1
//
//import org.apache.flink.api.common.functions.{RichFlatMapFunction, RichMapFunction}
//import org.apache.flink.api.common.state._
//import org.apache.flink.configuration.Configuration
//import org.apache.flink.streaming.api.scala.{StreamExecutionEnvironment, createTypeInformation}
//import org.apache.flink.util.Collector
//
//import java.util
//
//object StateTest {
//  def main(args: Array[String]): Unit = {
//    val env = StreamExecutionEnvironment.getExecutionEnvironment
//    env.setParallelism(1)
//
//    //    val inputDataStream = env.readTextFile("E:\\workspace\\FlinkExercise\\src\\main\\resources\\seneor.txt")
//    val inputDataStream = env.socketTextStream("localhost", 777)
//
//    val dataStream = inputDataStream
//      .map(data => {
//        val arr = data.split(",")
//        SensorReading(arr(0), arr(1).toLong, arr(2).toDouble)
//      })
//
//    // 需求：对于温度传感器温度值的跳变，超过10度，报警
//    val alertStream = dataStream.keyBy(_.id)
////      .flatMap(new learning1.TempChangeAlert(10.0))
//      .flatMapWithState[(String, Double, Double), Double]( {     // 带状态的flatMap
//        case (data: SensorReading, None) => (List.empty, Some(data.temperature))
//        case (data: SensorReading, lastTemp: Some[Double]) => {
//          // 跟最新的温度值求插值作比较
//          val diff = (data.temperature - lastTemp.get).abs
//          if(diff > 10.0)
//            (List((data.id, lastTemp.get, data.temperature)), Some(data.temperature))
//          else
//            (List.empty, Some(data.temperature))
//        }
//      })
//    alertStream.print()
//
//
//    env.execute("state test")
//  }
//}
//
//// 实现自定义RichFlatMapFunction
//class TempChangeAlert(threshold: Double) extends RichFlatMapFunction[SensorReading, (String, Double, Double)] {
//
//  // 定义状态保存上一次的温度值
//  lazy val lastTempState: ValueState[Double] = getRuntimeContext.getState(new ValueStateDescriptor[Double]("last-temp", classOf[Double]))
//
//  override def flatMap(value: SensorReading, out: Collector[(String, Double, Double)]): Unit = {
//    val lastTemp = lastTempState.value()  // 先获取上一次的温度值
//    val diff = (value.temperature - lastTemp).abs  // 温度差
//    if(diff > threshold)  // 还需要判断是否是初始值，可以设置一个非常大的初始值，或者通过Boolean
//      out.collect((value.id, lastTemp, value.temperature))
//
//    // 更新状态，以备下次使用
//    lastTempState.update(value.temperature)
//  }
//}
//
//// Keyed state 测试： 必须定义在RichFunction中，因为需要运行时候的上下文
//class MyRichMapper extends RichMapFunction[SensorReading, String] {
//
//  var valueState: ValueState[Double] = _
//  lazy val listSate: ListState[Int] = getRuntimeContext.getListState(new ListStateDescriptor[Int]("listState", classOf[Int]))
//
//  lazy val mapState: MapState[String, Double] = getRuntimeContext.getMapState(
//    new MapStateDescriptor[String, Double]("mapState",classOf[String], classOf[Double]))
//
//  lazy val reduceState: ReducingState[SensorReading] = getRuntimeContext.getReducingState(
//    new ReducingStateDescriptor[SensorReading]("reducingState", new MyReducer, classOf[SensorReading]))
//
//// 在open的生命周期里面，才能执行上下文获取当前状态
//  override def open(parameters: Configuration): Unit = {
////    lazy val valueState: ValueState[Double] = getRuntimeContext.getState(new ValueStateDescriptor[Double]("valueState", classOf[Double]))
//    valueState = getRuntimeContext.getState(new ValueStateDescriptor[Double]("valueState", classOf[Double]))
//  }
//
//  override def map(value: SensorReading): String = {
//    // 状态的读写
//    val myValue = valueState.value()
//    valueState.update(value.temperature)  // 更改
//
//    listSate.add(1)  // 追加Int
//    val list = new util.ArrayList[Int]()
//    list.add(2)
//    list.add(3)
//    listSate.addAll(list)  // 追加List
//    listSate.update(list)  // 旧的替换为新的list
//    listSate.get()  // 货期当前的list
//
//    mapState.contains("sensor_1")
//    mapState.get("sensor_1")
//    mapState.put("sensor_1", 1.3)
//
//    reduceState.get()  // 根据MyReducer 聚合完之后的SensorReading
//    reduceState.add(value)  // 传入value并聚合
//
//
//    value.id
//  }
//
//}