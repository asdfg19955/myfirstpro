package day17;

import redis.clients.jedis.Jedis;

import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 *  Redis的类型基本操作
 */
public class TestJedis {
    public static void main(String[] args) {
        Jedis jedis = new Jedis("192.168.28.131", 6379);
        jedis.auth("123");
        // 清理下数据库
        jedis.flushDB();
        /**
         * 字符串操作
         */
        // 添加一个字符串
        jedis.set("bigdata","db");
        System.out.println(jedis.get("bigdata"));
        // 在原来可以对应的值追加一个字符串，如果key不存在，则创建一个新的key
        jedis.append("bigdata","test");
        System.out.println(jedis.get("bigdata"));
        // 自增1， 等价于count ++ 默认值为0
        jedis.incr("count");
        // 在原来的值基础上增加指定的值
        jedis.incrBy("count",10);
        System.out.println(jedis.get("count"));
        jedis.incrByFloat("double",1.5);
        System.out.println(jedis.get("double"));
        // 将value值重新赋值
        jedis.getSet("count","100");
        System.out.println(jedis.get("count"));
        // 获取字符串的长度
        Long len = jedis.strlen("bigdata");
        System.out.println(len);
        // 同时设置多个key的值
        jedis.mset("bigdata","java","count","2000");
        List<String> mget = jedis.mget("bigdata", "count");
        System.out.println(mget);
        /**
         * 集合操作 Set
         * 元素不可重复，无序
         */
        // 添加元素
        Long sadd = jedis.sadd("hobbys", "学习", "玩", "喝", "旅游", "看书");
        System.out.println(sadd);
        // 查询集合元素数量
        jedis.scard("hobbys");
        // 取多个集合的差集
        jedis.sadd("hobbys2","旅游","玩");
        Set<String> sdiff = jedis.sdiff("hobbys", "hobbys2");
        System.out.println(sdiff);
        // 取交集
        Set<String> sinter = jedis.sinter("hobbys", "hobbys2");
        // 判断某个元素是否在集合当中
        Boolean sismember = jedis.sismember("hobbys", "玩");
        // 删除元素
        Long srem = jedis.srem("hobbys", "玩", "喝");
        /**
         * Zset 集合
         * 不可重复 元素有序
         *
         */
        // 添加元素（会按照score排序，值越大，排名越靠后）
        jedis.zadd("price",1,"1");
        jedis.zadd("price",5,"10");
        jedis.zadd("price",2,"5.2");
        jedis.zadd("price",6,"6");
        jedis.zadd("price",7,"3");
        jedis.zadd("price",10,"五百");
        // 获取元素
        Set<String> price = jedis.zrange("price", 0, -1);
        System.out.println(price);
        // 获取指定范围的元素数量
        Long price1 = jedis.zcount("price", 1, 8);
        System.out.println(price1);
        // 指定键在集合的score排名，下标从0开始算起
        Long zrank = jedis.zrank("price", "1");
        System.out.println(zrank);
        jedis.zrem("price","五百","3");
        // 获取指定范围的后几个元素(降序获取)
        Set<String> prive = jedis.zrevrange("price", 0, 1);
        System.out.println(prive);
        /**
         * List 队列
         */
        // 向头部添加元素
        Long lpush = jedis.lpush("users", "zhangweijian", "dengchao", "wangzulan", "guodegang");
        // 向尾部添加
        Long rpush = jedis.rpush("users", "zhouxingchi", "shenteng", "huangbo", "wumengda", "wujing");
        // 查看元素个数
        Long users = jedis.llen("users");
        System.out.println(users);
        // 查询指定区间的元素
        List<String> users1 = jedis.lrange("users", 0, -1);
        System.out.println(users1);
        // 删除指定元素
        jedis.lrem("users",1,"wumengda");
        // 插入元素
        jedis.lpushx("users","zhangsan");
        // 获取队列元素范围值
        jedis.ltrim("users",0,5);
        // 以下标获取元素值
        jedis.lindex("users",0);
        /**
         * hash 散列
         */
        jedis.hset("user","name","zhangsan");
        jedis.hset("user","age","18");
        // 获取key的值
        jedis.hget("user","name");
        // 获取所有的值
        Map<String, String> user = jedis.hgetAll("user");
        for(Map.Entry<String,String> entry :user.entrySet()){
            System.out.println(entry.getKey() + " = " + entry.getValue());
        }
        // 累加操作
        jedis.hincrBy("user","age",10);
        jedis.hincrByFloat("user","age",1.5);
        // 获取字段的长度
        jedis.hlen("user");
        jedis.hmget("user","name","age");
        // 获取所有字段值
        jedis.hvals("user");
    }
}
