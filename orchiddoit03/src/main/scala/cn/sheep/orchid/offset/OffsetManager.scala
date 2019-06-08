package cn.sheep.orchid.offset

import cn.sheep.orchid.config.ConfigHelper
import org.apache.kafka.common.TopicPartition
import org.apache.spark.streaming.kafka010.OffsetRange
import scalikejdbc._

/** 维护kafka偏移量
  * author: old sheep
  * QQ: 64341393 
  * Created 2019/1/11
  */
object OffsetManager {

	/**
	  * 将偏移量存储到MySQL
	  *
	  * @param offsets
	  * @param groupId
	  * @return
	  */
	def offset2MySQL(offsets: Array[OffsetRange], groupId: String = ConfigHelper.groupId) = {
		val params: Seq[Seq[Any]] = offsets.map(or => Seq(groupId, or.topic, or.partition, or.untilOffset)).toSeq
		DB.localTx { implicit session =>
			SQL("replace into orchid_offset_doit03 values(?,?,?,?)").batch(params: _*).apply()
		}
	}

	/**
	  * 获取数据库中偏移量
	  * @return
	  */
	def obtainOffset4MySQL = {
		DB.readOnly { implicit session =>
			SQL("select * from orchid_offset_doit03 where groupId =? and topic = ?")
    			.bind(ConfigHelper.groupId, ConfigHelper.topics.head)
    			.map(rs => (
					new TopicPartition(rs.string("topic"), rs.int("partitionId")),
					rs.long("offsets")
				)).list().apply()
		}.toMap
	}

}
