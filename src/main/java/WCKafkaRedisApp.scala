import java.{lang, util}

import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.kafka.common.TopicPartition
import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.log4j.{Level, Logger}
import org.apache.spark.SparkConf
import org.apache.spark.streaming.dstream.InputDStream
import org.apache.spark.streaming.kafka010._
import org.apache.spark.streaming.{Seconds, StreamingContext}
import redis.clients.jedis.Jedis

object WCKafkaRedisApp {

  Logger.getLogger("org").setLevel(Level.WARN)

  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setMaster("local[*]").setAppName("xx")
      //每秒钟每个分区kafka拉取消息的速率
      .set("spark.streaming.kafka.maxRatePerPartition", "100")
      // 序列化
      .set("spark.serilizer", "org.apache.spark.serializer.KryoSerializer")
      // 建议开启rdd的压缩
      .set("spark.rdd.compress", "true")
    val ssc = new StreamingContext(conf, Seconds(2))

    //启动一参数设置
    val groupId = "testgroup"
    val kafkaParams = Map[String, Object](
      "bootstrap.servers" -> "kafaka.devtest.com:9092",
      "key.deserializer" -> classOf[StringDeserializer],
      "value.deserializer" -> classOf[StringDeserializer],
      "group.id" -> groupId,
      "auto.offset.reset" -> "earliest",
      "enable.auto.commit" -> (false: lang.Boolean)
    )
    val topics = Array("testkafaka")

    //启动二参数设置
    var formdbOffset: Map[TopicPartition, Long] = JedisOffset(groupId)

    //拉取kafka数据
    val stream: InputDStream[ConsumerRecord[String, String]] = if (formdbOffset.size == 0) {
      KafkaUtils.createDirectStream[String, String](
        ssc,
        LocationStrategies.PreferConsistent,
        ConsumerStrategies.Subscribe[String, String](topics, kafkaParams)
      )
    } else {
      KafkaUtils.createDirectStream(
        ssc,
        LocationStrategies.PreferConsistent,
        ConsumerStrategies.Assign[String, String](formdbOffset.keys, kafkaParams, formdbOffset)

      )
    }


    //数据偏移量处理。
    stream.foreachRDD({
      rdd =>
        //获得偏移量对象数组
        val offsetRange: Array[OffsetRange] = rdd.asInstanceOf[HasOffsetRanges].offsetRanges

        //逻辑处理
        rdd.flatMap(_.value().split(" ")).map((_, 1)).reduceByKey(_ + _).foreachPartition({
          it =>
            val jedis = RedisClient.pool.getResource
            it.foreach({
              v =>
                print("wordcount:"+v)
                jedis.hincrBy("wordcount", v._1, v._2.toLong)
            }
            )
            jedis.close()
        })

        //偏移量存入redis
        val jedis: Jedis = RedisClient.pool.getResource
        for (or <- offsetRange) {
          jedis.hset(groupId, or.topic + "-" + or.partition, or.untilOffset.toString)
        }
        jedis.close()

    })


    ssc.start()
    ssc.awaitTermination()
  }
}

