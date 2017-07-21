package zetyun

import java.util.Properties

import zetyun.functions.{BootstrapFunction, CounterFunction, FilterFunction, QualifierFunction}
import zetyun.models.{ControlEvent, CustomerEvent, FilteredEvent}
import java.util.{Properties, UUID}
import zetyun.serialization.{ControlEventSchema, CustomerEventSchema}
import org.apache.flink.api.scala._
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.streaming.connectors.kafka.{FlinkKafkaConsumer09, FlinkKafkaProducer09}
/**
  * Created by ryan on 17-7-21.
  */
object Job {
  def main(args: Array[String]) {
    // set up the execution environment
    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)     // set Parallelism 1

    // set up the kafka properties
    val properties = new Properties()
    properties.setProperty("bootstrap.servers", "192.168.1.81:6667")

    // generate filterFunc, qualifierFunction, counterFunc, bootstrapFunc
    val filterFunction = new FilterFunction
    val qualifierFunction = new QualifierFunction
    val counterFunction = new CounterFunction
    val bootstrapFunction = new BootstrapFunction

    // add historical datasource
    val bootstrapStream = env.addSource(new FlinkKafkaConsumer09("bootstrap", new ControlEventSchema(), properties))
      .filter(x => x.isDefined)   // value is noEmpty
      .map(x => x.get)            // get value
      .flatMap(bootstrapFunction).name("Bootstrap Function")  // event stream rename
      .keyBy((fe: FilteredEvent) => { fe.event.customerId } ) // hash by every events.customerId

    val bootstrapSink = new FlinkKafkaProducer09("bootstrap", new ControlEventSchema(), properties)

    val eventStream = env.addSource(new FlinkKafkaConsumer09("events", new CustomerEventSchema(), properties))
      .filter(x => x.isDefined)   // value is noEmpty
      .map(x => x.get)            // get value
      .keyBy((ce: CustomerEvent) => { ce.customerId } ) //hash by every events.customerId

    val controlStream = env.addSource(new FlinkKafkaConsumer09("controls", new ControlEventSchema(), properties))
      .filter(x => x.isDefined)  // value is nonEmpty
      .map(x => x.get)           // get value
      .name("Control Source")    // rename Stream "Control Stream"
      .split((ce: ControlEvent) => {
      ce.customerId match {
        case Constants.GLOBAL_CUSTOMER_ID => List("global")
        case _ => List("specific")
      }
    })

    val globalControlStream = controlStream.select("global").broadcast

    val specificControlStream = controlStream.select("specific").keyBy((ce: ControlEvent) => {ce.customerId})

    val filterStream = globalControlStream.union(specificControlStream)
      .connect(
        eventStream
      )            // union control stream and event stream
      .flatMap(filterFunction).name("Filtering Function")     // filter by FilterFunction and rename "Filtering Function"
      .union(bootstrapStream)               // union real-time stream and historical stream stored by /resources/events.txt
      .flatMap(qualifierFunction).name("Qualifier Function")    // qualifier by Qualifier Function
      .flatMap(counterFunction).name("Counter Function")        // count by Counter Function
      .addSink(bootstrapSink)

    env.execute("FlinkForward")





  }

}
