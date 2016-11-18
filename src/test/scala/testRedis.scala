import com.bigdata.nciae.JedisConnectionPool
import redis.clients.jedis.Jedis

/**
 * Created by Rainbow on 2016/11/18.
 */
object testRedis {

  def main(args: Array[String]) {
    val connection: Jedis = JedisConnectionPool.getConnection()
    connection.set("xiaohzhang", "shuai")
    val s: String = connection.get("xiaohzhang")
    println(s)
    connection.close()

  }

}
