package cn.sheep.orchid.utils

import cn.sheep.orchid.config.ConfigHelper
import redis.clients.jedis.{JedisPool, JedisPoolConfig}

/**
  * author: old sheep
  * QQ: 64341393 
  * Created 2019/1/11
  */
object Jpools {

	private val jedisPoolConfig = new JedisPoolConfig()
	jedisPoolConfig.setMaxTotal(2000)
	jedisPoolConfig.setMaxIdle(150)
	jedisPoolConfig.setMinIdle(50)
	jedisPoolConfig.setTestOnBorrow(true)
	jedisPoolConfig.setTestOnCreate(true)
	jedisPoolConfig.setTestOnReturn(true)
	jedisPoolConfig.setTestWhileIdle(true)
	// 用的时候记得度娘一下
	private lazy val jedisPool = new JedisPool(jedisPoolConfig, ConfigHelper.redisHost, ConfigHelper.redisPort)

	/**
	  * 从池子中返回一个连接
	  * @return
	  */
	def getJedis = jedisPool.getResource

}
