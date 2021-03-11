//package learning1
//
//import org.apache.flink.configuration.Configuration
//import org.apache.flink.streaming.api.functions.sink.{RichSinkFunction, SinkFunction}
//import org.apache.flink.streaming.api.scala._
//
//import java.sql.{Connection, DriverManager, PreparedStatement}
//
//object jdbcSinkTest {
//  def main(args: Array[String]): Unit = {
//    val env = StreamExecutionEnvironment.getExecutionEnvironment
//    env.setParallelism(1)
//
//    val inputStreamFromFile: DataStream[String] = env.readTextFile("E:\\workspace\\FlinkExercise\\src\\main\\resources\\seneor.txt")
//
//    val dataStream = inputStreamFromFile.map(data => {
//      val arr = data.split(",")
//      SensorReading(arr(0), arr(1).toLong, arr(2).toDouble)
//    })
//
//    dataStream.addSink(new MyJdbcSinkFunc())
//
//    env.execute("jdbc sink test")
//  }
//}
//
//class MyJdbcSinkFunc() extends RichSinkFunction[SensorReading] {
//  // 定义连接，预编译语句
//  var conn: Connection = _
//  var insertStatement: PreparedStatement = _
//  var updateStatement: PreparedStatement = _
//
//  override def open(parameters: Configuration): Unit = {
//    conn = DriverManager.getConnection("jdbc:mysql://localhost:3306/test", "root", "123456")
//    insertStatement = conn.prepareStatement("insert into sensor_temp (id, temp) value (?, ?)")
//    updateStatement = conn.prepareStatement("update sensor_temp set temp = ? where id = ?")
//  }
//
//   def invoke(value: SensorReading, context: SinkFunction.Context[_]): Unit = {
//    // 先执行更新操作，查到就更新
//    updateStatement.setDouble(1, value.temperature)
//    updateStatement.setString(2, value.id)
//    updateStatement.execute()
//    // 如果更新没有查到数据，那么就插入
//    if (updateStatement.getUpdateCount == 0) {
//      insertStatement.setString(1, value.id)
//      insertStatement.setDouble(2, value.temperature)
//      insertStatement.execute()
//    }
//
//  }
//
//  override def close(): Unit = {
//    insertStatement.close()
//    updateStatement.close()
//    conn.close()
//
//  }
//}