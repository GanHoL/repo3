package cn.sheep.orchid.metric

import cn.sheep.orchid.comm.Constant
import cn.sheep.orchid.config.ConfigHelper
import cn.sheep.orchid.utils.{Jpools, OrchidUtils}
import com.alibaba.fastjson.JSON
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.spark.rdd.RDD

/** 业务的计算逻辑
  * author: old sheep
  * QQ: 64341393 
  * Created 2019/1/13
  */
object OrchidKPILogic {

	/**
	  * 整理需要计算的数据格式
	  * @param rdd
	  * @return
	  */
	def markBaseData(rdd: RDD[ConsumerRecord[String, String]]) = {
		// 过滤出需要处理的日志类型
		val filtered = rdd.map(crd => JSON.parseObject(crd.value()))
			.filter(_.getString(Constant.SERVICE).equals(Constant.RECHARGENOTIFYREQ))

		// reduceByKey -> RDD[(K, V)] -> RDD[(维度, 指标)] ->RDD[(天, (1, succ, moeny, time))]
		filtered.map(line => {
			val requestId = line.getString(Constant.REQUESTID)
			// 提取天
			val day = requestId.substring(0, 8)
			val hour = requestId.substring(8, 10)
			val minute = requestId.substring(10, 12)

			// 省份的编码
			val provinceCode = line.getString(Constant.PROVINCECODE)

			val (succ, money, costTime) = if (line.getString(Constant.BUSSINESSRST).equals("0000")) {
				val money = line.getString(Constant.CHARGEFEE).toDouble
				// 充值时长
				val endTime = line.getString(Constant.RECEIVENOTIFYTIME)
				(1L, money, OrchidUtils.caculateTime(requestId.substring(0, 17), endTime))
			} else (0L, 0d, 0L)

			(day, (1, succ, money, costTime), hour, provinceCode, minute)
		}).cache()
	}

	/**
	  * 计算业务概况 - 并将结果写入到redis
	  * @param baseData
	  */
	def calculateGeneratorDay(baseData: RDD[(String, (Int, Long, Double, Long), String, String, String)]) = {
		/*业务概况*/
		baseData.map(tp => (tp._1, tp._2)).reduceByKey((tp1, tp2) => {
			(
				tp1._1 + tp2._1,
				tp1._2 + tp2._2,
				tp1._3 + tp2._3,
				tp1._4 + tp2._4
			)
		}).foreachPartition(iter => {
			// jedis
			val jedis = Jpools.getJedis
			iter.foreach(tp => {
				jedis.hincrBy("orchid-" + tp._1, "total", tp._2._1)
				jedis.hincrBy("orchid-" + tp._1, "succ", tp._2._2)
				jedis.hincrByFloat("orchid-" + tp._1, "money", tp._2._3)
				jedis.hincrBy("orchid-" + tp._1, "time", tp._2._4)
			})
			jedis.close()
		})
	}

	/**
	  *
	  * @param baseData
	  */
	def calculateGeneratorHour(baseData: RDD[(String, (Int, Long, Double, Long), String, String, String)]) = {
		/*计算每个小时的充值订单量及成功率*/
		baseData
			.map(tp => ((tp._1, tp._3), (tp._2._1, tp._2._2)))
			.reduceByKey((tp1, tp2) => (tp1._1 + tp2._1, tp1._2 + tp2._2))
			.foreachPartition(partition => {
				val jedis = Jpools.getJedis
				partition.foreach(tp => {
					jedis.hincrBy("orchid-" + tp._1._1, tp._1._2 + "-total", tp._2._1)
					jedis.hincrBy("orchid-" + tp._1._1, tp._1._2 + "-succ", tp._2._2)
				})
				jedis.close()
			})
	}

	/**
	  *
	  * @param baseData
	  */
	def calculateProvinceDay(baseData: RDD[(String, (Int, Long, Double, Long), String, String, String)]) = {
		/*全国充值数据分布*/
		baseData.map(tp => ((tp._1, tp._4), tp._2._2))
			.reduceByKey(_ + _)
			.foreachPartition(partition => {
				val jedis = Jpools.getJedis
				val dictMap = ConfigHelper.pcode2nameDictMap
				partition.foreach(tp => {
					/*省份的编码转换成省份的名称 -> 省份编码和名称的映射关系 Map*/
					val pname = dictMap.get(tp._1._2).asInstanceOf[String]
					jedis.hincrBy("orchid-map-" + tp._1._1, pname, tp._2)
				})
				jedis.close()
			})
	}

	/**
	  *
	  * @param baseData
	  */
	def calculatePerMinute(baseData: RDD[(String, (Int, Long, Double, Long), String, String, String)]) = {
		/*每分钟的充值笔数及金额*/
		baseData.map(tp => ((tp._1, tp._3, tp._5), (tp._2._2, tp._2._3)))
			.reduceByKey((tp1, tp2) => {
				(tp1._1 + tp2._1, tp1._2 + tp2._2)
			})
			.foreachPartition(partition => {
				val jedis = Jpools.getJedis
				partition.foreach(tp => {
					jedis.hincrBy("orchid-" + tp._1._1, "succ-" + tp._1._2 + tp._1._3, tp._2._1)
					jedis.hincrByFloat("orchid-" + tp._1._1, "money-" + tp._1._2 + tp._1._3, tp._2._2)
				})
				jedis.close()
			})
	}



}
