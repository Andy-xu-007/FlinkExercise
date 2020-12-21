import org.apache.flink.api.common.serialization.SimpleStringEncoder
import org.apache.flink.core.fs.Path
import org.apache.flink.streaming.api.datastream.DataStreamSource
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment
import org.apache.flink.streaming.api.functions.sink.filesystem.StreamingFileSink

object FileSink {
  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)  // 切片

    val inputStreamFromFile: DataStreamSource[String] = env.readTextFile("E:\\workspace\\FlinkExercise\\src\\main\\resources\\seneor.txt")

    val dataStream = inputStreamFromFile.map(data => {
      val arr = data.split(",")
      SensorReading(arr(0), arr(1).toLong, arr(2).toDouble)
    })

//    dataStream.print()
//    dataStream.writeAsCsv("E:\\workspace\\FlinkExercise\\src\\main\\resources\\fileSink.txt")

    dataStream.addSink(StreamingFileSink[SensorReading])
    dataStream.addSink(StreamingFileSink.forRowFormat(
      new Path("E:\\workspace\\FlinkExercise\\src\\main\\resources\\seneor.txt"),
      new SimpleStringEncoder[SensorReading]()  // 默认是UTF-8
    ).build()
    )

    env.execute("file sink test")
  }
}
