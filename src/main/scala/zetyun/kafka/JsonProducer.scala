package zetyun.kafka

import java.util.Properties

import org.apache.kafka.clients.producer._

/**
  * Created by ryan on 17-7-17.
  */
class JsonProducer {
  val topic = "JsonSteam"
  val props = new Properties()
  props.put("bootstrap.servers", "192.168.1.81:6667")
  props.put("group.id","JsonPath")
  props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer")
  props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer")
  val producer = new KafkaProducer[String, String](props)
  val message = "{'store':{'persons':[{'name':'yx', 'age':'22'}, {'name':'xx', 'age':23}]}}"
  val record = new ProducerRecord(topic, "key", message)
  System.out.println(record)
  producer.send(record)
  producer.close()
}
