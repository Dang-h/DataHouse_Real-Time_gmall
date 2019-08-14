package app

import java.text.SimpleDateFormat
import java.util
import java.util.Date

import bean.StartUpLog
import com.alibaba.fastjson.JSON
import constant.GmallConstants
import org.apache.hadoop.yarn.webapp.example.MyApp.MyController
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.spark.SparkConf
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.rdd.RDD
import org.apache.spark.streaming.dstream.{DStream, InputDStream}
import org.apache.spark.streaming.{Seconds, StreamingContext}
import redis.clients.jedis.Jedis
import utils.MyKafkaUtil

object DauApplication {

  def main(args: Array[String]): Unit = {
    val sparkConf: SparkConf = new SparkConf().setAppName("dau_app").setMaster("local[*]")

    val ssc: StreamingContext = new StreamingContext(sparkConf, Seconds(5))

    //
    val inputDStream: InputDStream[ConsumerRecord[String, String]] = MyKafkaUtil.getKafkaStream(GmallConstants.KAFKA_TOPIC_STARTUP, ssc)

    //取k-v，k为自定义Id（作用：分区），v为json字符串
    //	inputDStream.foreachRDD{
    //	  rdd => println(rdd.map(_.value()).collect().mkString("\n"))
    //	}

    //统计每日活跃用户
    //去重，以mid为单位

    //转换结构，将json转换成样例类并将时间戳转换为日期，添加Date和Hour
    val startupLogDStream: DStream[StartUpLog] = inputDStream.map {
      record => {
        //提取json字符串内容
        val startupJsonString: String = record.value()
        //解析Json
        val startupLog: StartUpLog = JSON.parseObject(startupJsonString, classOf[StartUpLog])

        //转换时间戳(年-月-日 时)
        val dateTimeString: String = new SimpleDateFormat("yyyy-MM-dd HH").format(new Date(startupLog.ts))

        //添加date和Hour
        startupLog.logDate = dateTimeString.split(" ")(0)
        startupLog.logHour = dateTimeString.split(" ")(1)

        startupLog
      }
    }

    //根据mid去重，记录每天访问过的mid，形成一个列表存入Redis，形成dau-date-mid，一对多结构，mid不重复，使用set。形成一个访问清单
    //在存入Redis之前先过滤掉不符合结构的数据

    startupLogDStream.transform {
      rdd => {
        println("过滤前数据量： " + rdd.count())

        //每个执行周期查询Redis获取清单，通过广播变量发送到Executor
        //建立Redis连接
        val jedis: Jedis = new Jedis("hadoop102", 6379)

        val dauKey: String = "dau: " + new SimpleDateFormat("yyyy-MM-dd").format(new Date())
        val dauSet: util.Set[String] = jedis.smembers(dauKey)
        //创建广播变量
        val dauBC: Broadcast[util.Set[String]] = ssc.sparkContext.broadcast(dauSet)
        //释放Redis连接
        jedis.close()

        //过滤数据
        val filteredRDD: RDD[StartUpLog] = rdd.filter {
          startupLog => !dauBC.value.contains(startupLog.mid)
        }

        println("过滤后数据量： " + filteredRDD.count())
        filteredRDD
      }
    }

    //过滤后数据存入Redis
    val realFilterDStream: Unit = startupLogDStream.foreachRDD {
      rdd => {

        rdd.foreachPartition {
          startupItr => {
            //建立redis连接,在executor中创建连接，一次
            val jedis: Jedis = new Jedis("hadoop102", 6379)

            for (log <- startupItr) {
              //设计key，dau:2019-08-13 value
              val dauKey: String = "dau:" + log.logDate
              println(dauKey + ":::" + log.mid)

              //向redis中存入数据
              jedis.sadd(dauKey, log.mid)
            }

            //释放连接
            jedis.close()
          }
        }
      }
    }
    realFilterDStream

    println("流程启动")
    ssc.start()
    ssc.awaitTermination()
  }

}
