package bean

case class AlertInfo(mid: String, //设备id
					 uids: java.util.HashSet[String], //领取过优惠券的用户id
					 itemIds: java.util.HashSet[String], //优惠券涉及商品id
					 events: java.util.List[String], //行为
					 ts: Long) //发生预警时间戳