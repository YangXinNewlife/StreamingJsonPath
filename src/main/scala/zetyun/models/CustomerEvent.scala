package zetyun.models

import java.util.UUID

/**
  * Created by ryan on 17-7-21.
  */
case class CustomerEvent(customerId: UUID, payload: String)

object CustomerEvent extends ObjectMapperTrait[CustomerEvent]{
  def apply(s:String): CustomerEvent = {
    CustomerEvent.fromJson(s)
  }
}
