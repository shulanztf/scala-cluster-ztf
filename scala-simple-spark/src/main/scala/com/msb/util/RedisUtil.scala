package com.msb.util

import redis.clients.jedis.{Jedis, JedisPool}
import redis.clients.jedis.util.Pool

object RedisUtil {
  private[this] var jedisPool: Pool[Jedis] = _

  def init(host: String, port: Int): Unit = {
    jedisPool = new JedisPool(host, port)
  }

  def updateHot(dbIndex:Int,key: String, itemID:String): Boolean = {
    try {
      val jedis = jedisPool.getResource
      jedis.select(dbIndex)
      jedis.zincrby(key,1,itemID)
      jedis.close()
      true
    } catch {
      case e: Exception => {
        false
      }
    }
  }

  def insertValue(dbIndex:Int,key: String, field:String, value:String): Boolean = {
    try {
      val jedis = jedisPool.getResource
      jedis.select(dbIndex)
      jedis.hset(key,field,value)
      jedis.close()
      true
    } catch {
      case e: Exception => {
        false
      }
    }
  }


}
