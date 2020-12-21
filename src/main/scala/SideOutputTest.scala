import org.apache.flink.api.scala.createTypeInformation
import org.apache.flink.streaming.api.functions.ProcessFunction
import org.apache.flink.streaming.api.scala.{OutputTag, StreamExecutionEnvironment}
import org.apache.flink.util.Collector

// 侧输出流
object SideOutputTest {
  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)

    //    val inputDataStream = env.readTextFile("E:\\workspace\\FlinkExercise\\src\\main\\resources\\seneor.txt")
    val inputDataStream = env.socketTextStream("localhost", 777)

    val dataStream = inputDataStream
      .map(data => {
        val arr = data.split(",")
        SensorReading(arr(0), arr(1).toLong, arr(2).toDouble)
      })

    // 主流
    val highTempStream = dataStream.process(new SplitTempProcessor(30.0))

    highTempStream.print()
//    highTempStream.getSideOutput(new OutputTag[SensorReading]("low"))
    highTempStream.getSideOutput(new OutputTag[(String, Long, Double)]("low")).print("low")
    env.execute("side output test")
  }
}

// 实现自定义PrecessFunction，进行分流
class SplitTempProcessor(threshold: Double) extends ProcessFunction[SensorReading, SensorReading] {
  override def processElement(value: SensorReading, ctx: ProcessFunction[SensorReading, SensorReading]#Context, out: Collector[SensorReading]): Unit = {
    // 如果当前温度值大于30，则输出到主流
    if (value.temperature > threshold) {
      out.collect(value)
    } else {
      // 如果不超过30度，则输出到侧输出流
      ctx.output(new OutputTag[(String, Long, Double)]("low"), (value.id, value.timetmp, value.temperature))
    }
  }
}
