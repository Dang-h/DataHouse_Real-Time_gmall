package app

import bean.OrderInfo
import com.alibaba.fastjson.JSON
import constant.GmallConstants
import org.apache.hadoop.conf.Configuration
import org.apache.kafka.clients.consumer.ConsumerRecord

import org.apache.spark.streaming.dstream.{DStream, InputDStream}
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.{SparkConf, SparkContext}
import utils.MyKafkaUtil

object OderApp {
  def main(args: Array[String]): Unit = {

    val sparkConf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("OrderApp")
    val ssc = new StreamingContext(sparkConf, Seconds(5))

    //从kafka获取数据
    val inputDStream: InputDStream[ConsumerRecord[String, String]] = MyKafkaUtil.getKafkaStream(GmallConstants.KAFKA_TOPIC_NEW_ORDER, ssc)

    //补充时间戳
    //敏感字段脱敏（电话收件人地址···）
    val oderDStream: DStream[Unit] = inputDStream.map {
      record => {
        //转成JSON对象以便处理（bean中添加case class）
        val orderInfo: OrderInfo = JSON.parseObject(record.value(), classOf[OrderInfo])

        //补充时间戳字段
        //获取创建日期字段：2019-08-02 06:00:25
        val dateArr: Array[String] = orderInfo.create_time.split(" ")
        //日期
        orderInfo.create_date = dateArr(0)
        //小时
        orderInfo.create_hour = dateArr(1).split(":")(0)

        //数据脱敏
        //13888745858 =>1388*******
        val tuple: (String, String) = orderInfo.consignee_tel.splitAt(4)
        orderInfo.consignee_tel = tuple._1 + "*******"
      }
    }

    //TODO 增加一个额外的字段：  是否是用户首次下单 IS_FIRST_CONSUME

    import org.apache.phoenix.spark._
    //保存到HBase + Phoenix
    oderDStream.foreachRDD {
      rdd => {
        rdd.saveToPhoenix("gmall0311_order_info",
          Seq("ID", "PROVINCE_ID", "CONSIGNEE", "ORDER_COMMENT",
            "CONSIGNEE_TEL", "ORDER_STATUS", "PAYMENT_WAY",
            "USER_ID", "IMG_URL", "TOTAL_AMOUNT", "EXPIRE_TIME",
            "DELIVERY_ADDRESS", "CREATE_TIME", "OPERATE_TIME",
            "TRACKING_NO", "PARENT_ORDER_ID", "OUT_TRADE_NO",
            "TRADE_BODY", "CREATE_DATE", "CREATE_HOUR"),
          new Configuration, Some("hadoop102,hadoop103,hadoop104:2181"))
      }
    }

    ssc.start()
    ssc.awaitTermination()
  }

}

