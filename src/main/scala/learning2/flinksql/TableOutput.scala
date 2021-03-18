package learning2.flinksql

import org.apache.flink.streaming.api.scala._
import org.apache.flink.table.api.{DataTypes, EnvironmentSettings, FieldExpression, Table, UnresolvedFieldExpression, WithOperations}
import org.apache.flink.table.api.bridge.scala._
import org.apache.flink.table.descriptors.{Csv, FileSystem, Schema}

object TableOutput {
  def main(args: Array[String]): Unit = {
    // 1，创建表的执行环境
    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    // 如果并行度不设置为1，那么后面writeAsCsv将会写入到多个文件，out.csv将会是文件夹
    env.setParallelism(1)

    val bsStreamSettings = EnvironmentSettings.newInstance()
      .useBlinkPlanner()
      .inStreamingMode()
      .build()
    val bsStreamTableEnv = StreamTableEnvironment.create(env, bsStreamSettings)

    val inputPath = "E:\\workSpace\\FlinkExercise\\src\\main\\resources\\seneor.txt"

    bsStreamTableEnv
      .connect(new FileSystem().path(inputPath))
      .withFormat(new Csv())
      .withSchema(new Schema()
        .field("id", DataTypes.STRING())
        .field("timestamp", DataTypes.BIGINT())
        .field("temp", DataTypes.DOUBLE())
      )
      .createTemporaryTable("inputTable")

    // 3，转换操作
    val seneorTable: Table = bsStreamTableEnv.from("inputTable")
    val resultTable: Table = seneorTable
      .select($"id", $"temp")
//      .filter($"id" === "sensor_1")

    // 3.2 聚合算子
    val aggTable = seneorTable
      .groupBy($"id")
      .select($"id", $"id".count() as $"count")

    // 4, 输出到文件

    resultTable.toAppendStream[(String, Long)].print("test1")

    // 第一次group之后，每来一条就更新当前的数据，旧数据显示false,新数据显示true
//    aggTable.toRetractStream[]


    env.execute(" table out test")

  }

}
