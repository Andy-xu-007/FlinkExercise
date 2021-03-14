package learning2.sink

import learning2.SensorReading
import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.functions.sink.{RichSinkFunction, SinkFunction}
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.streaming.api.scala._

import java.sql.{Connection, DriverManager, PreparedStatement}

object JdbcSink {
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

    dataStream.addSink(new MyJdbcSinkFunc())


    env.execute("jdbs sink")
  }
}

class MyJdbcSinkFunc() extends RichSinkFunction[SensorReading]{
  // 定义连接, 预编译语句
  var conn: Connection = _
  var insertStmt: PreparedStatement = _
  var updateStmt: PreparedStatement = _


  override def open(parameters: Configuration): Unit = {
    conn = DriverManager.getConnection("jdbc:mysql://node01:3306/test", "root", "123456")
    insertStmt = conn.prepareStatement("insert into sensor_temp (id, temp) values (?, ?)")
    updateStmt = conn.prepareStatement("update sersor_temp set temp = ? where id = ?")
  }

  override def invoke(value: SensorReading, context: SinkFunction.Context): Unit = {
    // 先执行更新操作
    updateStmt.setDouble(1, value.temperature)
    updateStmt.setString(2,value.id)
    updateStmt.execute()

    // 如果更新没有查到数据，那么就插入
    if (updateStmt.getUpdateCount == 0) {
      insertStmt.setString(1, value.id)
      insertStmt.setDouble(2, value.temperature)
      insertStmt.execute()
    }
  }

  override def close(): Unit = {
    insertStmt.close()
    updateStmt.close()
    conn.close()
  }
}
