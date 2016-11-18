package com.bigdata.nciae

import redis.clients.jedis.{Jedis, JedisPool, JedisPoolConfig}

/**
 * Created by Rainbow on 2016/11/17.
 * Jedis 连接池
 */
object JedisConnectionPool extends  Serializable{

  val jedisConfig = new JedisPoolConfig()
  //最大连接数
  jedisConfig.setMaxTotal(10)
  //最大的空闲连接数
  jedisConfig.setMaxIdle(5)
  //当调用borrow object 是否进行有效的检查
  jedisConfig.setTestOnBorrow(true)


  val pool = new JedisPool(jedisConfig, "192.168.145.200", 7000)

  def getConnection(): Jedis = {
    pool.getResource
  }
}
