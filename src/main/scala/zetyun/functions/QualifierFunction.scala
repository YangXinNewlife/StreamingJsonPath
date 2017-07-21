package zetyun.functions

import zetyun.models.{FilteredEvent, QualifiedEvent}
import com.jayway.jsonpath.JsonPath
import org.apache.flink.api.common.functions.FlatMapFunction
import org.apache.flink.util.Collector
import scala.util.Try


/**
  * Created by ryan on 17-7-21.
  */

class QualifierFunction extends FlatMapFunction[FilteredEvent, QualifiedEvent] {
  override def flatMap(value: FilteredEvent, out: Collector[QualifiedEvent]): Unit = {
    Try(JsonPath.parse(value.event.payload)).map(ctx => {           // usr Json path filter the qualifier stream
      value.controls.foreach(control => {
        Try {
          val result: String = ctx.read(control.jsonPath)           // read the regix filter the ctx stream

          if (!result.isEmpty) {                                    // if result is nonEmpty
            out.collect(QualifiedEvent(value.event, control))       // output the after filter stream
          }
        }
      })
    })
  }
}
