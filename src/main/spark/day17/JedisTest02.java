package day17;

import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPool;

/**
 * 连接池Jedis
 */
public class JedisTest02 {
    public static void main(String[] args) {
        // 创建JedisPool
        JedisPool pool = new JedisPool("192.168.28.131", 6379);
        //  通过pool获取jedis实例
        Jedis jedis = pool.getResource();
        // 密码验证
        jedis.auth("123");
        System.out.println(jedis.get("a1"));
        // 关闭
        jedis.close();
    }
}
