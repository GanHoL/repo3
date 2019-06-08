package cn.sheep.orchid.config

import java.util

import com.typesafe.config.{Config, ConfigFactory, ConfigObject}
import org.apache.kafka.common.serialization.StringDeserializer
import scalikejdbc.config.DBs

/** 解析application.conf配置文件
  * author: old sheep
  * QQ: 64341393 
  * Created 2019/1/11
  */
object ConfigHelper {

	/**
	  * 加载 mysql配置
	  */
	DBs.setup()

	/**
	  * 加载application.conf文件
	  */
	private lazy val application: Config = ConfigFactory.load()

	/**
	  * kafka Servers
	  */
	val brokers = application.getString("orchid.kafka.brokers")

	/**
	  * groupId
	  */
	val groupId = application.getString("orchid.kafka.groupId")

	/**
	  * topics
	  */
	val topics = application.getString("orchid.kafka.topics").split(",")

	/**
	  * redis host
	  */
	val redisHost = application.getString("orchid.redis.host")

	/**
	  * redis port
	  */
	val redisPort = application.getInt("orchid.redis.port")

	/**
	  * kafka params
	  */
	val kafkaParams = Map[String, Object](
		"bootstrap.servers" -> ConfigHelper.brokers,
		"key.deserializer" -> classOf[StringDeserializer],
		"value.deserializer" -> classOf[StringDeserializer],
		"group.id" -> groupId,
		"auto.offset.reset" -> "earliest",
		"enable.auto.commit" -> (false: java.lang.Boolean)
	)

	/**
	  * 省份编码和名称的映射关系
	  */
	val pcode2nameDictMap = application.getObject("pcode2pname").unwrapped()

}
