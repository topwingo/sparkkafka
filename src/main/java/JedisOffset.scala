import java.util

import org.apache.kafka.common.TopicPartition

object JedisOffset {


  def apply(groupId: String) = {
    var formdbOffset = Map[TopicPartition, Long]()
    val jedis1 = RedisClient.pool.getResource
    val topicPartitionOffset: util.Map[String, String] = jedis1.hgetAll(groupId)
    import scala.collection.JavaConversions._
    val topicPartitionOffsetlist: List[(String, String)] = topicPartitionOffset.toList
    for (topicPL <- topicPartitionOffsetlist) {
      val split: Array[String] = topicPL._1.split("[-]")
      formdbOffset += (new TopicPartition(split(0), split(1).toInt) -> topicPL._2.toLong)
    }
    formdbOffset
  }
}
