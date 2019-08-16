package app

import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.{SparkConf, SparkContext}

object OderApp {
  def main(args: Array[String]): Unit = {

    val sparkConf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("OrderApp")
    val ssc = new StreamingContext(sparkConf, Seconds(5))

    //从kafka获取数据

    //补充时间戳
    //敏感字段脱敏（电话收件人地址···）
      //转成JSON对象以便处理（bean中添加case class）

      //补充时间戳字段


      //数据脱敏

    //保存到HBase + Phoenix

    ssc.start()
    ssc.awaitTermination()

  }

}
