package zetyun.serialization


import scala.util.{Failure, Success, Try}
import zetyun.models.ControlEvent
import org.apache.flink.api.common.typeinfo.{BasicTypeInfo, TypeInformation}
import org.apache.flink.streaming.util.serialization.{DeserializationSchema, SerializationSchema}

/**
  * Created by ryan on 17-7-21.
  */
class ControlEventSchema extends DeserializationSchema[Option[ControlEvent]] with SerializationSchema[ControlEvent]{


  // judge is End of Stream
  override def isEndOfStream(nextElement: Option[ControlEvent]): Boolean = {
    false
  }

  // deserialize the json to Object
  override def deserialize(message: Array[Byte]): Option[ControlEvent] = {
    val jsonString = new String(message, "UTF-8")

    Try(ControlEvent.fromJson(jsonString)) match {
      case Success(controlEvent) => Some(controlEvent)
      case Failure(ex) => None
    }

  }

  override def serialize(element: ControlEvent): Array[Byte] = {
    ControlEvent.toJson(element).map(_.toByte).toArray
  }

  override def getProducedType: TypeInformation[Option[ControlEvent]] = {
    BasicTypeInfo.getInfoFor(classOf[Option[ControlEvent]])
  }
}
