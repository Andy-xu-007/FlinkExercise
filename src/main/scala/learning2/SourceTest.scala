package learning2

import org.apache.flink.api.common.serialization.SimpleStringSchema
import org.apache.flink.streaming.api.scala.{DataStream, StreamExecutionEnvironment}
import org.apache.flink.api.scala._
import org.apache.flink.streaming.api.functions.source.SourceFunction
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer

import java.util.Properties
import scala.util.Random

object SourceTest {
  def main(args: Array[String]): Unit = {
    // 创建执行环境
    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment

    // 1，从集合中读取数据
    val dataList = List(
      SensorReading("sensor_1", 1547718199, 35.8),
      SensorReading("sensor_6", 1547718201, 15.4),
      SensorReading("sensor_7", 1547718202, 6.7),
      SensorReading("sensor_10", 1547718205, 38.1)
    )

    val stream1: DataStream[SensorReading] = env.fromCollection(dataList)
    stream1.print()

    // 2，可以用于测试
    val stream2 = env.fromElements(1.0, 35, "hello", Array(1, 2, 3, 4, 5))
    stream2.print()

    // 3，从文件中读取数据
    val inputPath = "E:\\workSpace\\FlinkExercise\\src\\main\\resources\\seneor.txt"
    val stream3 = env.readTextFile(inputPath)
    stream3.print()

    // 4, 从kafka读取数据
    val props = new Properties()
    props.setProperty("bootstrap.servers", "node01:9092")
    props.setProperty("group.id", "consumer-group")
    val stream4 = env.addSource(new FlinkKafkaConsumer[String](
      "sensor", new SimpleStringSchema(), props))

//    stream4.print()

    // 5, 自定义数据
   val stream5 = env.addSource(new MySensorSource())
    stream5.print()


    env.execute("source test")
  }

}

// 定义样例类，温度传感器
case class SensorReading(id: String, timeStamp: Long, temperature: Double)

// 自定义source function
class MySensorSource() extends SourceFunction[SensorReading]{

  // 定义一个标志位flag，用来表示数据源是否正常运行发出数据
  var running = true

  override def run(ctx: SourceFunction.SourceContext[SensorReading]): Unit = {
    // 定义一个随机数发生器
    val rand = new Random()

    // 随机生成一组（10个）传感器的初始温度：（id, temp）
    var curTemp = 1.to(10).map(i => ("sensor_" + i, rand.nextDouble() * 100))

    // 定义无限循环，不停的产生数据，除非被cancel
    while (running) {
      // 在上次的数据基础上微调，更新温度值
      curTemp = curTemp.map(
        data => (data._1, data._2 + rand.nextGaussian())
      )
      // 获取当前时间戳，加入到当前数据中
      val curTime = System.currentTimeMillis()

      // 通过ctx.collect方法将数据发出去
      curTemp.foreach(
        data => ctx.collect(SensorReading(data._1, curTime, data._2))
      )

      // 控制一下速度, 500ms
      Thread.sleep(500)
    }

  }

  override def cancel(): Unit = running = false
}