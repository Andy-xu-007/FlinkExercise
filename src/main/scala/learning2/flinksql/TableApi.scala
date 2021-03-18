package learning2.flinksql

import learning2.SensorReading
import org.apache.flink.api.scala.ExecutionEnvironment
import org.apache.flink.runtime.io.network.buffer.Buffer.DataType
import org.apache.flink.streaming.api.scala._
import org.apache.flink.table.api.{AnyWithOperations, DataTypes, EnvironmentSettings, FieldExpression, Table, TableEnvironment}
import org.apache.flink.table.api.bridge.scala._
import org.apache.flink.table.descriptors.{Csv, FileSystem, Kafka, Schema}

object TableApi {
  def main(args: Array[String]): Unit = {
    // 1，创建表的执行环境
    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    // 如果并行度不设置为1，那么后面writeAsCsv将会写入到多个文件，out.csv将会是文件夹
    env.setParallelism(1)

//    // 1.1 基于老版本planner的流处理
//    val settingOld = EnvironmentSettings.newInstance()
//      .useOldPlanner()
//      .inStreamingMode()
//      .build()
//    val oldStreamTableEnv = StreamTableEnvironment.create(env, settingOld)
//
//    // 1.2 基于老版本的批处理
//    val batchEnv = ExecutionEnvironment.getExecutionEnvironment
//    val oldBatchTableEnv = BatchTableEnvironment.create(batchEnv)

    // 1.3 基于blink planner 的流处理
    val bsStreamSettings = EnvironmentSettings.newInstance()
      .useBlinkPlanner()
      .inStreamingMode()
      .build()
    val bsStreamTableEnv = StreamTableEnvironment.create(env, bsStreamSettings)

    // 1.4 基于blink planner 的批处理
    val bsBatchSettings = EnvironmentSettings.newInstance()
      .useBlinkPlanner()
      .inBatchMode()
      .build()
    val bsBatchTableEnv = TableEnvironment.create(bsBatchSettings)

    // 2,连接外部系统，读取数据，注册表
    // 2.1 读取文件
    val inputPath = "E:\\workSpace\\FlinkExercise\\src\\main\\resources\\seneor.txt"

    bsStreamTableEnv
      .connect(new FileSystem().path(inputPath))
      .withFormat(new Csv())
      .withSchema(new Schema()
        .field("id", DataTypes.STRING())
        .field("timestamp", DataTypes.BIGINT())
        .field("temperature", DataTypes.DOUBLE())
      )
      .createTemporaryTable("inputTable")

//    val inputTable: Table = bsStreamTableEnv.from("inputTable")
//    inputTable.toAppendStream[(String, Long, Double)].print()

    // 2.2 从kafka读取数据
    bsStreamTableEnv.connect( new Kafka()
      .version("universal") // 'connector.version' expects 'universal', but is '0.11'
      .topic("sensor")
      .property("zookeeper.connect", "node01:2181,node02:2181,node03:2181")
      .property("bootstrap.servers", "node01:9092")
    )
      .withFormat(new Csv)
      .withSchema(new Schema()
        .field("id", DataTypes.STRING())
        .field("timestamp", DataTypes.BIGINT())
        .field("temperature", DataTypes.DOUBLE())
      )
      .createTemporaryTable("kafkaInputTable")

//        val kafkaTable: Table = bsStreamTableEnv.from("kafkaInputTable")
//        kafkaTable.toAppendStream[(String, Long, Double)].print()

    // 3. 查询转换
    // 3.1 使用table api
    val sersorTable = bsStreamTableEnv.from("inputTable")
    sersorTable
//      .select("id, temperature")
      .select($"id",$"temperature")  // 1.10 -1.11 可以使用scala 的singal形式：'id, 'temperature
//      .filter($"id" === "sensor_1")  //有问题，待看官网解决

    // 3.2
    val resultSqlTable = bsStreamTableEnv.sqlQuery(
      """
        |select id, temperature
        |from inputTable
        |where id = 'sensor_1'
        |""".stripMargin
    )

    resultSqlTable.toAppendStream[(String, Double)].print("result")


    env.execute("table api test")
  }

}
