package yanbinwa.common.redis;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.TimeUnit;

import org.apache.log4j.Logger;

import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPool;
import redis.clients.jedis.JedisPoolConfig;
import redis.clients.jedis.exceptions.JedisConnectionException;
import yanbinwa.common.constants.CommonConstants;

/**
 * 
 * 这里实现了对于Redis操作的api，对象只要持有RedisClient，就可以进行redis进行操作了，注意redis内部是单线程的
 * 
 * @author yanbinwa
 *
 */

public class RedisClient
{
    private static final Logger logger = Logger.getLogger(RedisClient.class);
    
    private JedisPool jedisPool;
    private ThreadLocal<Jedis> jedis = new ThreadLocal<Jedis>();
    private BlockingQueue<Jedis> remainConnectionQueue = null;
    private Set<Jedis> runningJedisSet = new HashSet<Jedis>();
    private int connectionMaxNum = -1;
    
    public RedisClient(String ip, int port, int maxTotal, int maxIdle, long maxWait, boolean testOnBorrow)
    {
        JedisPoolConfig config = new JedisPoolConfig();
        config.setMaxTotal(maxTotal);
        config.setMaxIdle(maxIdle); 
        config.setMaxWaitMillis(maxWait); 
        config.setTestOnBorrow(testOnBorrow); 
        jedisPool = new JedisPool(config, ip, port);
        
        connectionMaxNum = maxTotal;
        remainConnectionQueue = new ArrayBlockingQueue<Jedis>(connectionMaxNum);
        for (int i = 0; i < connectionMaxNum; i ++)
        {
            Jedis jedis = jedisPool.getResource();
            if (jedis == null)
            {
                logger.error("Jedis connection should not be null");
                continue;
            }
            remainConnectionQueue.add(jedis);
        }
    }
    
    /**
     * 从queue中获取一个redis，可以是阻塞的，必须调用的，如何保证线程安全？？？
     * @throws InterruptedException 
     * 
     */
    public boolean getJedisConnection() throws InterruptedException
    {
        if (jedis.get() != null)
        {
            logger.info("Has already get the redis connection");
            return true;
        }
        Jedis connection = remainConnectionQueue.poll(CommonConstants.REDIS_GET_CONNECTION_TIMEOUT, TimeUnit.MILLISECONDS);
        if (connection == null)
        {
            logger.error("getJedisConnection should not be null");
            return false;
        }
        jedis.set(connection);
        runningJedisSet.add(connection);
        logger.trace("Get connection from queue");
        return true;
    }
    
    public boolean returnJedisConnection()
    {
        if (jedis.get() == null)
        {
            logger.error("Threadlocal jedis should not be null");
            return false;
        }
        Jedis connection = jedis.get();
        runningJedisSet.remove(connection);
        boolean ret = remainConnectionQueue.offer(connection);
        if (!ret)
        {
            logger.error("input connection failed");
            return false;
        }
        jedis.set(null);
        return true;
    }
    
    /**
     * 
     * 这里要关闭打开的connection，之后再做jedisPool close
     * 
     */
    public void closePool()
    {
        List<Jedis> jedisConnection = new ArrayList<Jedis>();
        int retry = 0; 
        while (runningJedisSet.size() != 0 && retry < CommonConstants.REDIS_CLOSE_RETRY_TIMES)
        {
            logger.info("There are still thread using the jedis connection");
            try
            {
                Thread.sleep(100);
            } 
            catch (InterruptedException e)
            {
                e.printStackTrace();
            }
            retry ++;
        }
        if (runningJedisSet.size() > 0)
        {
            logger.error("Still have tread hold jedis connection, will force close them");
            jedisConnection.addAll(runningJedisSet);
        }
        jedisConnection.addAll(remainConnectionQueue);
        for (Jedis jedis : jedisConnection)
        {
            try
            {
                jedis.close();
            }
            catch (JedisConnectionException e1)
            {
                logger.error("Can not close the jedis");
                continue;
            }
        }
        if (jedisPool != null)
        {
            jedisPool.close();
        }
    }
    
    /**
     * 删除当前选择数据库中的所有key
     * 
     * @param jedis
     */
    public void flushDB()
    {
        jedis.get().flushDB();
    }
    
    public String setString(String key, String value)
    {
        return jedis.get().set(key, value);
    }
    
    public String getString(String key)
    {
        return jedis.get().get(key);
    }
    
    public void delString(String key)
    {
        jedis.get().del(key);
    }
    
    public void setList(String key, List<String> values)
    {
        jedis.get().lpush(key, (String[])values.toArray());
    }
    
    public List<String> getList(String key)
    {
        return jedis.get().lrange(key, 0, -1);
    }
    
    public void delList(String key)
    {
        jedis.get().ltrim(key, 0, -1);
    }
    
    public void setSortedSet(String key, Double score, String value)
    {
        jedis.get().zadd(key, score, value);
    }
    
    public void setSortedSet(String key, Map<String, Double>scoreMembers)
    {
        jedis.get().zadd(key, scoreMembers);
    }
    
    public Set<String> getSortedSet(String key) 
    {
        return jedis.get().zrange(key, 0, -1);
    }
    
    public Set<String> getSortedSet(String key, Double min, Double max)
    {
        return jedis.get().zrangeByScore(key, min, max);
    }
    
    public void delSortedSet(String key)
    {
        jedis.get().zrem(key);
    }
    
    public void delSortedSet(String key, Double start, Double end)
    {
        jedis.get().zremrangeByScore(key, start, end);
    }
    
    /**
     * 设置数据的到期时间
     * 
     * @param jedis
     * @param key
     * @param value
     * @param seconds
     */
    public void setStringWithExpireTime(String key, String value, int seconds)
    {
        jedis.get().setex(key, seconds, value);
    }
}
