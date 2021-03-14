package learning2.sink

import javassist.Loader.Simple
import learning2.SensorReading
import org.apache.flink.api.common.serialization.SimpleStringEncoder
import org.apache.flink.core.fs.Path
import org.apache.flink.streaming.api.functions.sink.filesystem.StreamingFileSink
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.streaming.api.scala._

object FileSink {
  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    // 如果并行度不设置为1，那么后面writeAsCsv将会写入到多个文件，out.csv将会是文件夹
    env.setParallelism(1)

    val inputPath = "E:\\workSpace\\FlinkExercise\\src\\main\\resources\\seneor.txt"

    val inputStream = env.readTextFile(inputPath)

    // 先转换成样例类
    val dataStream = inputStream.map(
      data => {
        val arr = data.split(",")
        SensorReading(arr(0), arr(1).toLong, arr(2).toDouble)
      }
    )

    dataStream.print()
//    dataStream.writeAsCsv("E:\\workSpace\\FlinkExercise\\src\\main\\resources\\out.csv")
    // 自定义sink

    dataStream.addSink(StreamingFileSink.forRowFormat(new Path("E:\\workSpace\\FlinkExercise\\src\\main\\resources\\out1.csv"),
      new SimpleStringEncoder[SensorReading]())
      .build()
    )

    env.execute("sink test")
  }

}
