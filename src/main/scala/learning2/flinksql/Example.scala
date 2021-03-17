package learning2.flinksql

import learning2.SensorReading
import org.apache.flink.streaming.api.scala._
import org.apache.flink.table.api.{EnvironmentSettings, Table}
import org.apache.flink.table.api.bridge.scala.{StreamTableEnvironment, tableConversions}

object Example {
  def main(args: Array[String]): Unit = {
    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
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

    // 创建表执行环境 -- 官网案例
    val bsSettings = EnvironmentSettings.newInstance().useBlinkPlanner().inStreamingMode().build()
    val tableEnv = StreamTableEnvironment.create(env, bsSettings)
    // 基于流创建表
    val dataTable: Table = tableEnv.fromDataStream(dataStream)
    // 调用table api进行转换
    val resultTable = dataTable
      .select("id, temperature")
      .filter("id == 'sensor_1'")

    // 直接用sql实现
    tableEnv.createTemporaryView("dataTable", dataTable)
    val sql = "select id, temperature from dataTable where id ='sensor_1'"
    val resultSqlTable = tableEnv.sqlQuery(sql)

    resultTable.toAppendStream[(String, Double)].print("result")
    resultSqlTable.toAppendStream[(String, Double)].print("result sql")


    env.execute("flink sql test")
  }

}
