package cn.sheep.orchid

import cn.sheep.orchid.comm.Constant
import cn.sheep.orchid.config.ConfigHelper
import cn.sheep.orchid.metric.OrchidKPILogic
import cn.sheep.orchid.offset.OffsetManager
import cn.sheep.orchid.utils.{Jpools, OrchidUtils}
import com.alibaba.fastjson.JSON
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.spark.SparkConf
import org.apache.spark.rdd.RDD
import org.apache.spark.streaming.dstream.InputDStream
import org.apache.spark.streaming.kafka010._
import org.apache.spark.streaming.{Seconds, StreamingContext}

/**
  * 业务主类
  * author: old sheep
  * QQ: 64341393 
  * Created 2019/1/11
  */
object OrchidApp {

	def main(args: Array[String]): Unit = {

		val sparkConf = new SparkConf()
			.setAppName("实时数据分析-orchid")
    		.set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
    		.set("spark.streaming.stopGracefullyOnShutdown", "true") // 优雅的关闭
    		.set("spark.streaming.kafka.maxRatePerPartition", "10000") // 控制最大的消费速率
			.setMaster("local[*]")

		val ssc = new StreamingContext(sparkConf, Seconds(1))

		val dstream: InputDStream[ConsumerRecord[String, String]] = KafkaUtils.createDirectStream(ssc,
			LocationStrategies.PreferConsistent,
			ConsumerStrategies.Subscribe[String, String](
				ConfigHelper.topics,
				ConfigHelper.kafkaParams,
				OffsetManager.obtainOffset4MySQL)
		)

		dstream.foreachRDD(rdd => {
			if (!rdd.isEmpty()) {
				// 获取offset -> 当前批次的偏移量 -> Array.size = Kafka Partition
				val offsetRanges: Array[OffsetRange] = rdd.asInstanceOf[HasOffsetRanges].offsetRanges
				val baseData = OrchidKPILogic.markBaseData(rdd)
				// 业务概况-按天统计的指标
				OrchidKPILogic.calculateGeneratorDay(baseData)
				// 业务概况-按小时统计的指标
				OrchidKPILogic.calculateGeneratorHour(baseData)
				OrchidKPILogic.calculateProvinceDay(baseData)
				OrchidKPILogic.calculatePerMinute(baseData)

				// 将offset存储到MySQL中
				OffsetManager.offset2MySQL(offsetRanges)
				baseData.unpersist(false)
			}
		})


		ssc.start()
		ssc.awaitTermination()
	}


}
