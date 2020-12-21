import java.lang

import org.apache.flink.api.java.tuple.Tuple
import org.apache.flink.streaming.api.datastream.DataStream
import org.apache.flink.streaming.api.functions.{AssignerWithPeriodicWatermarks, AssignerWithPunctuatedWatermarks}
import org.apache.flink.streaming.api.functions.windowing.WindowFunction
import org.apache.flink.streaming.api.watermark.Watermark
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.windowing.assigners.{EventTimeSessionWindows, TumblingProcessingTimeWindows}
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.api.windowing.windows.TimeWindow
import org.apache.flink.streaming.runtime.operators.util.AssignerWithPeriodicWatermarksAdapter
import org.apache.flink.util.Collector

import scala.collection.convert.ImplicitConversions.`iterable AsScalaIterable`

/**
 * Copyright (c) 2020-2030 All right Reserved
 *
 * @author :   Andy Xu
 * @version :   1.0
 * @date :   8/20/2020 8:58 PM
 *
 */
object windowTest {
  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)

    val inputStreamFromFile = env.readTextFile("E:\\workspace\\FlinkExercise\\src\\main\\resources\\seneor.txt")

    // 1，基本操作
    val dataStream = inputStreamFromFile
      .map(data => {
        val dataArray = data.split(",")
        SensorReading(dataArray(0), dataArray(1).toLong, dataArray(2).toDouble)
      })
      .assignTimestampsAndWatermarks( new MyWaterMarkAssigner_old(1000L))

    val resultStream1 = dataStream
      .keyBy("id")
      //      .window(EventTimeSessionWindows.withGap(Time.m
      //
      //      inutes(1)))  // 会话窗口，一分钟没有操作就会关闭
      //      .timeWindow(Time.hours(1), Time.minutes(1))   // 滑动窗口，一个参数是滚动窗口
      //      .window(TumblingProcessingTimeWindows.of(Time.days(1), Time.hours(8)))  // 标准时间偏移8小时
      //      .countWindow(10, 2) // 记数窗口，每10个隔两个再记数
      //      .reduce(new MyReduce())
      .timeWindow(Time.seconds(15), Time.seconds(5))
      // 要注意新老版本API内调用的不同
      .apply(new MyWindowFunction())
    val resultStream = resultStream1

    resultStream.print()
    env.execute("window test")

  }

  // 自定义全窗口函数
  class MyWindowFunction() extends WindowFunction[SensorReading, (Long, Int), Tuple, TimeWindow] {
    override def apply(key: Tuple, window: TimeWindow, input: lang.Iterable[SensorReading], out: Collector[(Long, Int)]): Unit = {
      out.collect((window.getStart, input.size))   // window.getStart当前窗口时间
    }
  }

  // 自定义周期性生成waterMark的Assigner，注意新旧API
  // 适用于比较密的数据
  class MyWaterMarkAssigner_old(lateness: Long) extends AssignerWithPeriodicWatermarks[SensorReading] {
    // 需要两个关键参数，延迟时间，和当前所有数据中最大的时间戳
//    val lateness = 1000L  // 1s
    var maxTs = Long.MinValue + lateness

    override def getCurrentWatermark: Watermark =
      new Watermark(maxTs - lateness)

    override def extractTimestamp(element: SensorReading, recordTimestamp: Long): Long = {
      maxTs = maxTs.max(element.timetmp * 1000L)
      element.timetmp * 1000L
    }
  }

  // 自定义一个断点式非周期生成waterMark的Assigner
  // 适用于没有时间规律的数据，稀疏数据
  class MyWaterMarkAssigner_2 extends AssignerWithPunctuatedWatermarks[SensorReading] {
    val lateness = 1000L
    override def checkAndGetNextWatermark(lastElement: SensorReading, extractedTimestamp: Long): Watermark = {
      if (lastElement.id == "sensder_1") {
        new Watermark(extractedTimestamp - lateness)
      } else
        null
    }

    override def extractTimestamp(element: SensorReading, recordTimestamp: Long): Long = {
      element.timetmp * 1000L
    }
  }

  class MyWaterMarkAssigner extends AssignerWithPeriodicWatermarksAdapter(AssignerWithPeriodicWatermarksAdapter)

}
