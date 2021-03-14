package learning2.sink

import learning2.SensorReading
import org.apache.flink.api.common.functions.RuntimeContext
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.connectors.elasticsearch.{ElasticsearchSinkFunction, RequestIndexer}
import org.apache.flink.streaming.connectors.elasticsearch7.ElasticsearchSink
import org.apache.http.HttpHost
import org.elasticsearch.client.Requests

import java.util

object EsSink {
  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment

    val inputPath = "E:\\workSpace\\FlinkExercise\\src\\main\\resources\\seneor.txt"

    val inputStream = env.readTextFile(inputPath)

    // 先转换成样例类
    val dataStream = inputStream.map(
      data => {
        val arr = data.split(",")
        SensorReading(arr(0), arr(1).toLong, arr(2).toDouble)
      }
    )

    // 定义HttpHosts
    val httpHosts = new util.ArrayList[HttpHost]()
    httpHosts.add(new HttpHost("node01",9200))
    httpHosts.add(new HttpHost("node02",9200))
    httpHosts.add(new HttpHost("node03",9200))

    // 自定义写入es的EsSinkFunction
    val myEsSinkFunc = new ElasticsearchSinkFunction[SensorReading] {
      override def process(element: SensorReading, ctx: RuntimeContext, indexer: RequestIndexer): Unit = {
        // 包装一个map作为data source
        val dataSource = new util.HashMap[String, String]()
        dataSource.put("id", element.id)
        dataSource.put("temperature", element.temperature.toString)
        dataSource.put("ts", element.timeStamp.toString)

        // 创建index request 用于发送请求
        val indexRequest = Requests.indexRequest()
          .index("sensor")  // 指定写入哪一个索引，必须是小写
//          .`type`("readingdata")  // es7不需要这个参数了
          .source(dataSource)  //指定写入的数据

        // 用indexer发送请求
        indexer.add(indexRequest)
      }
    }

    dataStream.addSink( new ElasticsearchSink
    .Builder[SensorReading](httpHosts,myEsSinkFunc)
    .build()
    )
    // kibana中查看：GET sensor/_search

    env.execute("es sink test")
  }

}
