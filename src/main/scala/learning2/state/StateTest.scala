package learning2.state

import learning2.{MyReducer, SensorReading}
import org.apache.flink.api.common.functions.RichMapFunction
import org.apache.flink.api.common.state._
import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.scala.{StreamExecutionEnvironment, _}

import java.{lang, util}

object StateTest {
  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    // 如果并行度不设置为1，那么后面writeAsCsv将会写入到多个文件，out.csv将会是文件夹
    env.setParallelism(1)

    val inputPath = "E:\\workSpace\\FlinkExercise\\src\\main\\resources\\seneor.txt"

//    val inputStream = env.readTextFile(inputPath)
    val inputStream = env.socketTextStream("node01", 9999)

    // 先转换成样例类
    val dataStream = inputStream.map(
      data => {
        val arr = data.split(",")
        SensorReading(arr(0), arr(1).toLong, arr(2).toDouble)
      }
    )

    env.execute("state test")
  }

}

// keyed state测试： 必须定义在RichFunction中，因为需要运行时的上下文
class MyRichMapper extends RichMapFunction[SensorReading, String] {
  var valueState: ValueState[Double] = _
  lazy val listState: ListState[Int] = getRuntimeContext
    .getListState(new ListStateDescriptor[Int]("liststate", classOf[Int]))
  lazy val mapState: MapState[String, Double] = getRuntimeContext
    .getMapState(new MapStateDescriptor[String, Double]("mapstate", classOf[String], classOf[Double]))
  lazy val reduceState: ReducingState[SensorReading] = getRuntimeContext
    .getReducingState(new ReducingStateDescriptor[SensorReading]("reducestate", new MyReducer, classOf[SensorReading]))

  override def open(parameters: Configuration): Unit = {
    valueState = getRuntimeContext
      .getState(new ValueStateDescriptor[Double]("valuestate", classOf[Double]))
  }

  override def close(): Unit = super.close()

  override def map(value: SensorReading): String = {
    // 获取状态
   val myValue = valueState.value()
    // 状态更新
    valueState.update(value.temperature)

    // 增加一个数
    listState.add(1)
    // 增加一个列表数据
    val list = new util.ArrayList[Int]()
    list.add(2)
    list.add(3)
    listState.addAll(list)
    //替换
    listState.update(list)
    // 获取
    val ints: lang.Iterable[Int] = listState.get()

    // 是否包含。。。
    mapState.contains("sensor_1")
    // 获取
    mapState.get("sensor_1")
    mapState.put("sensor_1", 1.3)

    reduceState.get()
    // value和当前数据聚合
    reduceState.add(value)

    value.id
  }
}
