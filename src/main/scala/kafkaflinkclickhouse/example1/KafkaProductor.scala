package kafkaflinkclickhouse.example1

import org.apache.kafka.clients.producer.{KafkaProducer, ProducerRecord}

import java.text.SimpleDateFormat
import java.util.{Date, Properties}
import scala.util.Random.shuffle

object KafkaProductor {
  def main(args: Array[String]): Unit = {
    SendKafka("first")
  }

  def SendKafka(topic: String): Unit = {
    // 创建kafka生产者的配置信息
    val props = new Properties()
    // 1，Kafka 服务端的主机名和端口号，"bootstrap.servers"替换为ProducerConfig就可以看到所有的参数
    props.put("bootstrap.servers", "node01:9092, node2:9092, node3:9092" )
    // 2，ack的应答级别：等待所有副本节点的应答
    props.put("acks", "all")
    // 3，消息发送最大尝试次数，即重试次数
    props.put("retries", "1")
    // 4，一次发送消息的批次大小：16K
    props.put("batch.size", "16384")
    // 5，等待时间  --- 4,5同为限制条件
    props.put("linger.ms", "1")
    // 6，发送缓存区内存大小：RecordAccumulator缓冲区大小：32M
    props.put("buffer.memory", "33554432");
    // 7，key 序列化类
    props.put("key.serializer",
      "org.apache.kafka.common.serialization.StringSerializer")
    // 8，value 序列化类
    props.put("value.serializer",
      "org.apache.kafka.common.serialization.StringSerializer")

    // 创建生产者对象
    val producer = new KafkaProducer[String, String](props)

    var member_id = Array.range(1, 10)
    var goods = Array("Milk", "Bread", "Rice", "Nodles", "Cookies", "Fish", "Meat", "Fruit", "Drink", "Books", "Clothes", "Toys")


    while (true) {
      var ts = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss").format(new Date())
      var msg = shuffle(member_id.toList).head + "\t" + shuffle(goods.toList).head + "\t" + ts + "\t" + "\n"
      print(msg)
      var record = new ProducerRecord[String, String](topic, msg)
      producer.send(record)
      Thread.sleep(2000)
    }
  }
}
