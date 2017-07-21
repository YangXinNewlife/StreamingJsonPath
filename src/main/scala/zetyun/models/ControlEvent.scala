package zetyun.models

import java.util.UUID


/**
  * Created by ryan on 17-7-21.
  */
case class ControlEvent(customerId: UUID, alertId: String, alertName:String, alertDescription: String, threshold: Int, jsonPath: String, bootstrapCustomerId: UUID)

object ControlEvent extends ObjectMapperTrait[ControlEvent]


