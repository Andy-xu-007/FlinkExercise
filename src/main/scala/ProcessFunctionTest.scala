import org.apache.flink.api.common.state.ValueStateDescriptor
import org.apache.flink.api.java.tuple.Tuple
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment
import org.apache.flink.streaming.api.functions.KeyedProcessFunction
import org.apache.flink.util.Collector

object ProcessFunctionTest {
  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)

    val inputStream = env.socketTextStream("localhost", 7777)
    val dataStream = inputStream.map(data => {
      val dataArray = data.split(",")
      SensorReading(dataArray(0), dataArray(1).toLong, dataArray(2).toDouble)
    })

    // 检测每一个传感器温度是否连续上升，在10秒之内
    val warningStream = dataStream
      .keyBy("id")
      .process(new TempIncreWarning(10000L))

    warningStream.print()
    env.execute("Process Function job")

  }
}

// 自定义KeyProcessFunction
class TempIncreWarning(interval: Long) extends KeyedProcessFunction[Tuple, SensorReading, String] {
  // 定义状态：保存上一个温度值进行比较，保存注册定时器的时间戳用于触发报警之后删除定时器
  lazy val lastTempState = getRuntimeContext.getState(new ValueStateDescriptor[Double]("lastTemp", classOf[Double]))
  // 为了方便删除定时器，还需要保存定时器的时间戳，
  lazy val curTimerTsState = getRuntimeContext.getState(new ValueStateDescriptor[Long]("Current temp Ts", classOf[Long]))
  override def processElement(value: SensorReading, ctx: KeyedProcessFunction[Tuple, SensorReading, String]#Context, out: Collector[String]): Unit = {
    // 首先取出上一次状态
    val lastTemp = lastTempState.value()
    val curTimerTs = curTimerTsState.value()

    // 将上次温度值的状态更新为当前数据的温度值
    lastTempState.update(value.temperature)

    // 判断当前温度值，如果比之前温度高，并且没有定时器，则注册当前时间十秒之后的定时器
    // 定时器默认是0，可以在上面指定
    if (value.temperature > lastTemp && curTimerTs == 0) {
      // 定时器的时间戳
      val ts = ctx.timerService().currentProcessingTime() + interval
      // 注册定时器
      ctx.timerService().registerProcessingTimeTimer(ts)
      // 更新状态
      curTimerTsState.update(ts)
    }
    // 如果温度下降，删除定时器
    else if (value.temperature < lastTemp) {
      // 删除定时器
      ctx.timerService().deleteProcessingTimeTimer(curTimerTs)
      // 清空状态
      curTimerTsState.clear()
//      curTimerTsState.update(0)  // 也可以设置为0
    }
  }

  // 下面是定义定时器触发的时候（10秒内没有来下降的温度值），所做的事情，报警
  override def onTimer(timestamp: Long, ctx: KeyedProcessFunction[Tuple, SensorReading, String]#OnTimerContext, out: Collector[String]): Unit = {
//    out.collect("温度值连续" + interval/1000 + "秒上升")
    out.collect("传感器" + ctx.getCurrentKey + "的温度连续" + interval/1000 + "连续上升")
    curTimerTsState.clear()
    // 如果有需要，还可以清空温度值状态
  }
}

