package kafkaflinkclickhouse.example1

import org.apache.flink.api.common.serialization.SimpleStringSchema
import org.apache.flink.connector.jdbc.{JdbcConnectionOptions, JdbcExecutionOptions, JdbcSink, JdbcStatementBuilder}
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer
import org.apache.flink.table.api.EnvironmentSettings
import org.apache.flink.table.api.bridge.scala._

import java.sql.PreparedStatement
import java.text.SimpleDateFormat
import java.util.Properties

object flinkJdbcClickHouse {
  def main(args: Array[String]): Unit = {
    val url = "jdbc:clickhouse://node01:8123/default"
    val user = "default"
    val passwd = "hOn0d9HT"
    val driver = "ru.yandex.clickhouse.ClickHouseDriver"
    val batchsize = 500 // 设置batch size，测试的话可以设置小一点，这样可以立刻看到数据被写入

    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    // 如果并行度不设置为1，那么后面writeAsCsv将会写入到多个文件，out.csv将会是文件夹
    env.setParallelism(1)
    // Flink 默认使用 ProcessingTime 处理,设置成event time
    // In Flink 1.12 the default stream time characteristic has been changed to TimeCharacteristic.EventTime
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)

    val bsStreamSettings = EnvironmentSettings.newInstance()
      .useBlinkPlanner()
      .inStreamingMode()
      .build()
    val bsStreamTableEnv = StreamTableEnvironment.create(env, bsStreamSettings)

    // kafka
    val props = new Properties()
    props.setProperty("bootstrap.servers", "node01:9092,node02:9092,node03:9092")
    props.setProperty("group.id", "consumer-group")
    props.setProperty("auto.offset.reset", "latest")
    props.setProperty("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer")
    props.setProperty("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer")

    val kafkaStream = env.addSource(new FlinkKafkaConsumer[String](
      "first", new SimpleStringSchema(), props))

    val resultStream = kafkaStream.map(line => {
      val x = line.split("\t")
      val member_id = x(0).trim.toLong
      val item = x(1).trim
      val times = x(2).trim
      var time = 0l
      try {
        // //时间戳类型
        time = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss")
          .parse(times).getTime

      } catch {
        case e: Exception => println(e.getMessage)
      }
      (member_id.toInt, item.toString, time.toLong)
    }).assignTimestampsAndWatermarks(new BoundedOutOfOrdernessTimestampExtractor[(Int, String, Long)](Time.seconds(3)) {
      override def extractTimestamp(t: (Int, String, Long)): Long = {
        t._3
      }
    }).map(x => {
      // 时间还原成datetime类型
      (x._1, x._2, new SimpleDateFormat("yyyy-MM-dd HH:mm:ss").format(x._3))
    })

    val sql = "insert into ChinaDW.testken(userid,items,create_date)values(?,?,?)"
    //当前版本的 flink-connector-jdbc，使用 Scala API 调用 JdbcSink 时会出现 lambda 函数的序列化问题。
    // 我们只能采用手动实现 interface 的方式来传入相关 JDBC Statement build 函数
    class CkSinkBuilder extends JdbcStatementBuilder[(Int, String, String)] {
      def accept(ps: PreparedStatement, v: (Int, String, String)): Unit = {
        ps.setInt(1, v._1)
        ps.setString(2, v._2)
        ps.setString(3, v._3)
      }

      resultStream.addSink(JdbcSink.sink[(Int, String, String)](sql, new CkSinkBuilder,
        new JdbcExecutionOptions.Builder().withBatchSize(5).build(),
        new JdbcConnectionOptions.JdbcConnectionOptionsBuilder()
          .withUrl("jdbc:clickhouse://node02:8123")
          .withDriverName("ru.yandex.clickhouse.ClickHouseDriver")
          .withUsername("default")
          .build()
      ))
    }

    env.execute("TO_CK")

  }
}
