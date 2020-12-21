import org.apache.flink.api.common.state.{ValueState, ValueStateDescriptor}
import org.apache.flink.api.scala.createTypeInformation
import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.functions.KeyedProcessFunction
import org.apache.flink.streaming.api.scala.{OutputTag, StreamExecutionEnvironment}
import org.apache.flink.util.Collector

object PrecessFunctionTest2 {
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
      .keyBy(_.id)
      .process(new MyKeyedProcessFunction)
  }
}

// KeyProcessFunction功能测试
class MyKeyedProcessFunction extends KeyedProcessFunction[String, SensorReading, String]{

  // 状态编程的操作也可以用在这里
  var myState: ValueState[Int] = _


  override def open(parameters: Configuration): Unit = {
    myState = getRuntimeContext.getState(new ValueStateDescriptor[Int]("myState", classOf[Int]))
  }

  override def processElement(value: SensorReading, ctx: KeyedProcessFunction[String, SensorReading, String]#Context, out: Collector[String]): Unit = {
    // ctx 是上下文
    ctx.getCurrentKey
    ctx.timestamp()
//    ctx.output(new OutputTag[String]())  // 侧输出流
    ctx.timerService().currentWatermark()
    ctx.timerService().registerEventTimeTimer(ctx.timestamp() + 60000L)  // 60秒之后执行
//    ctx.timerService().deleteEventTimeTimer()  // 删除时间戳
  }

  // 上面定时器触发的时候要做的操作，如果有多个定时器，都是在onTimer里面
  override def onTimer(timestamp: Long, ctx: KeyedProcessFunction[String, SensorReading, String]#OnTimerContext, out: Collector[String]): Unit = super.onTimer(timestamp, ctx, out)
}