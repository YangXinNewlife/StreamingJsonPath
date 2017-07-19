package zetyun.flink

import java.util.Properties

import com.jayway.jsonpath.JsonPath
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer010
import org.apache.flink.streaming.util.serialization.SimpleStringSchema
import org.apache.flink.api.scala._

/**
  * Created by ryan on 17-7-17.
  */
class FlinkKaConsumer {
  def main(args: Array[String]): Unit ={
    val topic = "JsonSteam"
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.enableCheckpointing(5000)
    val properties = new Properties()
    properties.setProperty("bootstrap.servers", "192.168.1.81:6667")
    properties.setProperty("zookeeper.connect", "192.168.1.81:2181")
    val simpleStrSchema = new SimpleStringSchema()
    val flinkkafconsumer = new FlinkKafkaConsumer010[String](topic, simpleStrSchema, properties)
    val stream = env.addSource(flinkkafconsumer)
    //stream.setParallelism(4)
    val age1 = JsonPath.read(stream, ".store.persons[0].age")
    val age2 = JsonPath.read(stream, ".store.persons[0].age")
    System.out.println("Ans is Person 1 and Person2 ages is : " + Integer.parseInt(age1) + Integer.parseInt(age2))
    env.execute("FlinkKafkaCount")
  }
}
