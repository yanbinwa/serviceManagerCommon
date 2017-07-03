package yanbinwa.common.redis;

import java.util.Set;

import org.junit.Test;

import redis.clients.jedis.Jedis;

public class RedisClientTest
{

    @Test
    public void test()
    {
        RedisClient rc = new RedisClient("192.168.56.17", 6379, 20, 5, 10000, false);
        
        Jedis jedis = rc.getJedisConnection();
        rc.flushDB(jedis);
        String key = "wyb";
        String[] names = {"wyb", "wzy", "zcl", "wjy"};
        long[] timestamp = {10000, 20000, 30000, 40000};
        for(int i = 0; i < names.length; i ++)
        {
            rc.setSortedSet(jedis, key, (double)timestamp[i], names[i]);
        }   
        Set<String> rets = rc.getSortedSet(jedis, key);
        for(String ret : rets)
        {
            System.out.println(ret);
        } 
        rc.returnJedisConnection(jedis);
        rc.closePool();
    }
    
    @Test
    public void expireTest()
    {
        RedisClient rc = new RedisClient("192.168.56.17", 6379, 20, 5, 10000, false);
        Jedis jedis = rc.getJedisConnection();
        rc.flushDB(jedis);
        rc.setStringWithExpireTime(jedis, "wyb", "123", 2);
        try
        {
            Thread.sleep(3000);
        } 
        catch (InterruptedException e)
        {
            e.printStackTrace();
        }
        String value = rc.getString(jedis, "wyb");
        System.out.println(value);
    }

}
