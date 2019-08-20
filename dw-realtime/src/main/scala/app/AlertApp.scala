package app

import java.util

import bean.{AlertInfo, EventInfo}
import com.alibaba.fastjson.JSON
import constant.GmallConstants
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.spark.SparkConf
import org.apache.spark.streaming.dstream.{DStream, InputDStream}
import org.apache.spark.streaming.{Seconds, StreamingContext}
import utils.MyKafkaUtil

import scala.util.control.Breaks._

/*
需求：同一设备，5分钟内三次及以上用不同账号登录并领取优惠劵，并且在登录到领劵过程中没有浏览商品。达到以上要求则产生一条预警日志。
同一设备，每分钟只记录一次预警。
 */

object AlertApp {


	def main(args: Array[String]): Unit = {
		val conf: SparkConf = new SparkConf().setAppName("AlertApp").setMaster("local[*]")
		val ssc = new StreamingContext(conf, Seconds(5))

		//读取Topic中的数据
		val inputStream: InputDStream[ConsumerRecord[String, String]] = MyKafkaUtil.getKafkaStream(GmallConstants.KAFKA_TOPIC_EVENT, ssc)

		//转换结构为一个JSON对象样例类
		val eventInfoDStream: DStream[EventInfo] = inputStream.map {
			record => {
				val jsonStr: String = record.value()
				val eventInfo: EventInfo = JSON.parseObject(jsonStr, classOf[EventInfo])
				eventInfo
			}
		}

		//设置滑动窗口，5分钟内--->窗口大小(步长<开窗)
		val eventWindowDStream: DStream[EventInfo] = eventInfoDStream.window(Seconds(300), Seconds(5))

		//同一设备，按照mid分组
		val groupByMidDStream: DStream[(String, Iterable[EventInfo])] = eventWindowDStream.map(eventInfo => (eventInfo.mid, eventInfo)).groupByKey()

		//判断预警
		//一个设备；1、三次及以上，不同设备，领取优惠券；2、没有浏览商品
		//变换结构为CouponAlertInfo， 判断是否符合预警要求， 给符合预警要求的mid打上标签
		val checkedDS: DStream[(Boolean, AlertInfo)] = groupByMidDStream.map {
			case (mid, eventInfoItr) => {

				//事件列表
				val eventList = new util.ArrayList[String]()
				//领券的用户
				val couponUidSet = new util.HashSet[String]()
				//领券的商品
				val itemSet = new util.HashSet[String]()
				//点击操作标签
				var hasClickItem = false

				breakable {
					for (eventInfo: AlertInfo <- eventInfoItr) {
						//收集mid的所有操作
						eventList.add(eventInfo.evid)
						//如果事件id为coupon的用户uid记录下来,领取过优惠券的商品记录
						if (eventInfo.evid == "coupon") {
							couponUidSet.add(eventInfo.uid)
							itemSet.add(eventInfo.itemid)
						}

						//给点击操作打标签
						if (eventInfo.evid == "clickItem") {
							hasClickItem = true
							break()
						}
					}
				}

				//判断是否符合预警条件: 同一设备领券用户>=3， 领券过程中没有浏览（点击）商品
				val flag: Boolean = couponUidSet.size >= 3 && !hasClickItem

				//组合成是否预警元组（标签，预警信息对象）
				(flag, AlertInfo(mid, couponUidSet, itemSet, eventList, System.currentTimeMillis()))
			}
		}

		//过滤调预警标签为true的预警信息
		val alterDS: DStream[AlertInfo] = checkedDS.filter(_._1).map(_._2)


		//保存数据到ES
		alterDS.foreachRDD {
			rdd =>
				rdd.foreachPartition {
					alterItr => {
						//将可迭代对象转为List
						val list: List[AlertInfo] = alterItr.toList

						//提取主键 mid + 分钟；同时也利用主键去重
						val alterListWithId: List[(String, AlertInfo)] = list.map { alterInfo =>
							(alterInfo.mid + "_" + alterInfo.ts / 1000 / 60, alterInfo)
						}

						//批量保存
						MyEsUtils.indexBulk(GmallConstants.ES_INDEX_ALTER, alterListWithId)
					}
				}
		}
	}
}
