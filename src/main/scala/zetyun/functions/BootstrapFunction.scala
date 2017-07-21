package zetyun.functions

import org.apache.flink.api.common.functions.FlatMapFunction
import org.apache.flink.util.Collector
import zetyun.models.{ControlEvent, CustomerEvent, FilteredEvent}

import scala.io.Source
/**
  * Created by ryan on 17-7-21.
  */

class BootstrapFunction extends FlatMapFunction[ControlEvent, FilteredEvent] {
  override def flatMap(value: ControlEvent, out: Collector[FilteredEvent]): Unit = {

    val stream = getClass.getResourceAsStream("/events.txt")  // load historical data into stream

    Source.fromInputStream(stream)                            // read stream
      .getLines                                               // read by line
      .toList                                                 // convert stream to a list
      .map(x => CustomerEvent(x))                             // use the Customer function
      .filter(x => x.customerId == value.bootstrapCustomerId) // fliter input.customerId equal x.customerId
      .foreach(x => {
      out.collect(FilteredEvent(x, List(value)))            // add to list, by FilteredEvent
    })
  }
}