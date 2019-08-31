package com.kafka

import java.lang

import com.alibaba.fastjson.JSON
import com.mchange.v2.c3p0.ComboPooledDataSource
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.kafka.common.TopicPartition
import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.log4j.{Level, Logger}
import org.apache.spark.SparkConf
import org.apache.spark.rdd.RDD
import org.apache.spark.streaming.dstream.InputDStream
import org.apache.spark.streaming.kafka010.{ConsumerStrategies, HasOffsetRanges, KafkaUtils, LocationStrategies}
import org.apache.spark.streaming.{Seconds, StreamingContext}


/**
  * Redis管理Offset
  */
object KafkaRedisOffset2 {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("offset").setMaster("local[2]")
      // 设置没秒钟每个分区拉取kafka的速率
      .set("spark.streaming.kafka.maxRatePerPartition", "100")
      .set("spark.driver.allowMultipleContext", "true")
      // 设置序列化机制
      .set("spark.serlizer", "org.apache.spark.serializer.KryoSerializer")
    val ssc = new StreamingContext(conf, Seconds(3))
    val jedis = JedisConnectionPool.getConnection()
    val sc = ssc.sparkContext

    //取消日志显示
    Logger.getLogger("org.apache.spark").setLevel(Level.OFF)


    // 配置参数
    // 配置基本参数
    //
    val groupId = "grou_bbbbb"
    // topic
    val topic = "bbb"
    // 指定Kafka的broker地址（SparkStreaming程序消费过程中，需要和Kafka的分区对应）
    val brokerList = "hadoop01:9092,hadoop02:9092,hadoop03:9092"
    // 编写Kafka的配置参数
    val kafkas = Map[String, Object](
      "bootstrap.servers" -> brokerList,
      // kafka的Key和values解码方式
      "key.deserializer" -> classOf[StringDeserializer],
      "value.deserializer" -> classOf[StringDeserializer],
      "group.id" -> groupId,
      // 从头消费
      "auto.offset.reset" -> "earliest",
      // 不需要程序自动提交Offset
      "enable.auto.commit" -> (false: lang.Boolean)
    )
    // 创建topic集合，可能会消费多个Topic
    val topics = Set(topic)
    // 第一步获取Offset
    // 第二步通过Offset获取Kafka数据
    // 第三步提交更新Offset
    // 获取Offset
    var fromOffset: Map[TopicPartition, Long] = JedisOffset(groupId)

    //切分字段并广播ip
    val ipinfo: RDD[(String, String, String)] = sc.textFile("dir/ip.txt")
      .map(line => {
        val fields = line.split("\\|")
        val startIP = fields(2)
        val endIP = fields(3)
        val province = fields(6)
        (startIP, endIP, province)
      })


    val broad = sc.broadcast(ipinfo.collect())


    // 判断一下有没数据
    val stream: InputDStream[ConsumerRecord[String, String]] =
      if (fromOffset.size == 0) {
        KafkaUtils.createDirectStream(ssc,
          // 本地策略
          // 将数据均匀的分配到各个Executor上面
          LocationStrategies.PreferConsistent,
          // 消费者策略
          // 可以动态增加分区
          ConsumerStrategies.Subscribe[String, String](topics, kafkas)
        )
      } else {
        // 不是第一次消费
        KafkaUtils.createDirectStream(
          ssc,
          LocationStrategies.PreferConsistent,
          ConsumerStrategies.Assign[String, String](fromOffset.keys, kafkas, fromOffset)
        )
      }

    val dataSource = new ComboPooledDataSource()
    stream.foreachRDD({
      rdd =>
        val offestRange = rdd.asInstanceOf[HasOffsetRanges].offsetRanges
        // 业务处理
        val baseData = rdd.map(_.value()).map(x => {
          val field = x.split(" ")

          val day = field(0).substring(0, 7)
          val ip = field(1)
          val tp = field(2)
          val detail = field(3)
          val money = field(4)


          val ipArr = broad.value
          //用户ip
          val userIp = ip2Long(ip)
          //通过二分查找，找到用户ip对应的省份
          val index = binarySearch(ipArr, userIp)

          val province = ipArr(index)._3


          (day, ip, tp, detail, money, province)

        })
        val result1: RDD[(String, Double)] = baseData.map(x => (x._1, x._5.toDouble)).reduceByKey(_ + _)
        Result01(result1)

        //问题2.计算每个商品分类的成交量的（结果保存到Redis中）
        val result2: RDD[(String, Double)] = baseData.map(x => ((x._1 + "_" + x._3), x._5.toDouble)).reduceByKey(_ + _)
        Result02(result2)

        //问题3.计算每个地域的商品成交总金额（结果保存到Redis中）

        val result3: RDD[(String, Double)] = baseData.map(x => ((x._1 + "_" + x._6), x._5.toDouble)).reduceByKey(_+_)

        Result03(result3)


        // 将偏移量进行更新

        for (or <- offestRange) {
          jedis.hset(groupId, or.topic + "-" + or.partition, or.untilOffset.toString)
        }

    })

    /**
      * 计算出总的成交量总额（结果保存到Redis中）
      */

    def Result01(rdd: RDD[(String, Double)]) = {
      rdd.foreachPartition(x => {
        val jedis = JedisConnectionPool.getConnection()
        jedis.select(8)
        x.foreach(x => {
          jedis.hincrBy(x._1, "allmoney", x._2.toLong)
        })
        jedis.close()
      })

    }

    def Result02(rdd: RDD[(String, Double)]) = {
      rdd.foreachPartition(x => {
        val jedis = JedisConnectionPool.getConnection()
        jedis.select(8)
        x.foreach(x => {
          jedis.hincrBy(x._1, "tpmoney", x._2.toLong)
        })
        jedis.close()
      })

    }

    def Result03(rdd: RDD[(String, Double)]) = {
      rdd.foreachPartition(x => {
        val jedis = JedisConnectionPool.getConnection()
        jedis.select(8)
        x.foreach(x => {
          jedis.hincrBy(x._1, "promoney", x._2.toLong)
        })
        jedis.close()
      })

    }

    /**
      * 把IP转化为long类型的数据
      *
      * @param ip
      * @return
      */
    def ip2Long(ip: String): Long = {
      val fragments = ip.split("[.]")
      var ipNum = 0L
      for (i <- 0 until fragments.length) {
        ipNum = fragments(i).toLong | ipNum << 8L
      }
      return ipNum
    }

    def binarySearch(arr: Array[(String, String, String)], ip: Long): Int = {
      //开始和结束值
      var start = 0
      var end = arr.length - 1
      while (start <= end) {
        //求中间值
        val middle = (start + end) / 2

        if ((ip >= arr(middle)._1.toLong) && (ip <= arr(middle)._2.toLong)) {
          return middle //return 在scala中作为中断,不写return则会继续往下执行，造成报错
        }
        if (ip < arr(middle)._1.toLong) {
          end = middle - 1
        } else {
          start = middle + 1
        }
      }
      -1
    }
    // 启动
    ssc.start()
    ssc.awaitTermination()


  }

}