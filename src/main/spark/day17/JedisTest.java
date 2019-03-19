package day17;

import redis.clients.jedis.Jedis;

/**
 * Jedis连接
 */
public class JedisTest {
    public static void main(String[] args) {
        // 创建Jedis
        Jedis jedis = new Jedis("192.168.28.131", 6379);
        // 输入密码，进行验证
        jedis.auth("123");
        System.out.println(jedis.ping());
        // 通过jedis进行存值和取值操作
        jedis.set("a1","Jedis Test");
        String a1 = jedis.get("a1");
        System.out.println(a1);
        // 关闭
        jedis.close();
    }
}
