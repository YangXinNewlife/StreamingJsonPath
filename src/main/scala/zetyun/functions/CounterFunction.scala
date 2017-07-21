package zetyun.functions

import zetyun.models.{ControlEvent, QualifiedEvent}
import org.apache.flink.api.common.functions.FlatMapFunction
import org.apache.flink.util.Collector


class CounterFunction extends FlatMapFunction[QualifiedEvent, ControlEvent] {
  var counts = scala.collection.mutable.HashMap[String, Int]()              // define a (String, int) map

  override def flatMap(value: QualifiedEvent, out: Collector[ControlEvent]): Unit = {
    val key = s"${value.event.customerId}${value.control.alertId}"          // key = customerId+alertId
    if (counts.contains(key)) {                                             // if qualifiedEvent.value.key in counts
      counts.put(key, counts.get(key).get + 1)                              // put the count + 1
      println(s"Count for ${key}: ${counts.get(key).get}")                  // print the key: countNumber
    } else {
      val c = value.control                                                 //  else
      counts.put(key, 1)                                                    //  put key to 1
      out.collect(ControlEvent(c.customerId, c.alertId, c.alertName, c.alertDescription, c.threshold, c.jsonPath, value.event.customerId))  // add collect
      println(s"Bootstrap count for ${key}: ${counts.get(key).get}")        //  print the key : countNumber
    }
  }
}