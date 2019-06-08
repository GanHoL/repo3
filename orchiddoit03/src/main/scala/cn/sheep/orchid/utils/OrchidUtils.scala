package cn.sheep.orchid.utils

import org.apache.commons.lang3.time.FastDateFormat


/**
  * author: old sheep
  * QQ: 64341393 
  * Created 2019/1/11
  */
object OrchidUtils {

	private val fastDateFormat = FastDateFormat.getInstance("yyyyMMddHHmmssSSS")

	/**
	  * 计算两个时间差
	  * @param start  yyyyMMddHHmmssSSS
	  * @param end    yyyyMMddHHmmssSSS
	  * @return 毫秒
	  */
	def caculateTime(start: String, end: String) = {
		fastDateFormat.parse(end).getTime - fastDateFormat.parse(start).getTime
	}

}
