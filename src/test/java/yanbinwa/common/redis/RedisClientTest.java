package yanbinwa.common.redis;

import static org.junit.Assert.*;

import java.util.Set;

import org.junit.Test;

import yanbinwa.common.constants.CommonConstantsTest;

public class RedisClientTest
{

    @Test
    public void test() throws InterruptedException
    {
        RedisClient rc = new RedisClient(CommonConstantsTest.TEST_SERVER_IP, CommonConstantsTest.TEST_REDIS_PORT, 20, 5, 10000, false);
        
        boolean isGetConnection = rc.getJedisConnection();
        if (!isGetConnection)
        {
            fail("can not get redisConnect");
            return;
        }

        rc.flushDB();
        String key = "wyb";
        String[] names = {"wyb", "wzy", "zcl", "wjy"};
        long[] timestamp = {10000, 20000, 30000, 40000};
        for(int i = 0; i < names.length; i ++)
        {
            rc.setSortedSet(key, (double)timestamp[i], names[i]);
        }   
        Set<String> rets = rc.getSortedSet(key);
        for(String ret : rets)
        {
            System.out.println(ret);
        } 
        rc.returnJedisConnection();
        rc.closePool();
    }
    
    @Test
    public void expireTest()
    {
        RedisClient rc = new RedisClient(CommonConstantsTest.TEST_SERVER_IP, CommonConstantsTest.TEST_REDIS_PORT, 20, 5, 10000, false);
        try
        {
            boolean ret = rc.getJedisConnection();
            if (!ret)
            {
                fail("Can not get the jedis from");
                return;
            }
            rc.flushDB();
            rc.setStringWithExpireTime("wyb", "123", 2);
            Thread.sleep(3000);
            String value = rc.getString("wyb");
            System.out.println(value);
        } 
        catch (InterruptedException e1)
        {
            // TODO Auto-generated catch block
            e1.printStackTrace();
        }
        finally
        {
            rc.returnJedisConnection();
        }
        rc.closePool();
    }
    
    @Test
    public void redisConnectionTest() throws InterruptedException
    {
        //这里在获取connection时会阻塞wait的时间，会抛出JedisConnectionException
        RedisClient rc = new RedisClient(CommonConstantsTest.TEST_SERVER_IP, CommonConstantsTest.TEST_REDIS_PORT, 5, 2, 100, false);
        System.out.println("redisConnectionTest start");
        for (int i = 0; i < 5; i ++)
        {
            boolean ret = rc.getJedisConnection();
            if (!ret)
            {
                fail("Can not get the connection");
            }
            rc.setString("test" + i, "" + i);
        }
        System.out.println("redisConnectionTest stop");
        rc.returnJedisConnection();
        rc.closePool();
    }
    
    @Test
    public void redisConnectionTest2() 
    {
        RedisClient rc = new RedisClient(CommonConstantsTest.TEST_SERVER_IP, CommonConstantsTest.TEST_REDIS_PORT, 1, 0, 100, false);
        try
        {
            rc.getJedisConnection();
            rc.setString("wyb", "123");
            Thread.sleep(2000);
            String value = rc.getString("wyb");
            if (!value.equals("123"))
            {
                fail("Do not get the correct value");
            }
            System.out.println("Get value: " + value);
        } 
        catch (InterruptedException e)
        {
            e.printStackTrace();
        }
        finally
        {
            rc.returnJedisConnection();
            rc.closePool();
        }
    }

}
