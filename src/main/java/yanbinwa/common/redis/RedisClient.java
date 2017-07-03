package yanbinwa.common.redis;

import java.util.List;
import java.util.Map;
import java.util.Set;

import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPool;
import redis.clients.jedis.JedisPoolConfig;

/**
 * 
 * 这里实现了对于Redis操作的api，对象只要持有RedisClient，就可以进行redis进行操作了，注意redis内部是单线程的
 * 
 * @author yanbinwa
 *
 */

public class RedisClient
{
    private JedisPool jedisPool;
    
    public Jedis getJedisConnection()
    {
        if (jedisPool != null)
        {
            return jedisPool.getResource();
        }
        return null;
    }
    
    public RedisClient(String ip, int port, int maxTotal, int maxIdle, long maxWait, boolean testOnBorrow)
    {
        JedisPoolConfig config = new JedisPoolConfig();
        config.setMaxTotal(maxTotal);
        config.setMaxIdle(maxIdle); 
        config.setMaxWaitMillis(maxWait); 
        config.setTestOnBorrow(testOnBorrow); 
        
        jedisPool = new JedisPool(config, ip, port);
    }
    
    public void returnJedisConnection(Jedis redis)
    {
        redis.close();
    }
    
    public void closePool()
    {
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
    public void flushDB(Jedis jedis)
    {
        jedis.flushDB();
    }
    
    public String setString(Jedis jedis, String key, String value)
    {
        return jedis.set(key, value);
    }
    
    public String getString(Jedis jedis, String key)
    {
        return jedis.get(key);
    }
    
    public void delString(Jedis jedis, String key)
    {
        jedis.del(key);
    }
    
    public void setList(Jedis jedis, String key, List<String> values)
    {
        jedis.lpush(key, (String[])values.toArray());
    }
    
    public List<String> getList(Jedis jedis, String key)
    {
        return jedis.lrange(key, 0, -1);
    }
    
    public void delList(Jedis jedis, String key)
    {
        jedis.ltrim(key, 0, -1);
    }
    
    public void setSortedSet(Jedis jedis, String key, Double score, String value)
    {
        jedis.zadd(key, score, value);
    }
    
    public void setSortedSet(Jedis jedis, String key, Map<String, Double>scoreMembers)
    {
        jedis.zadd(key, scoreMembers);
    }
    
    public Set<String> getSortedSet(Jedis jedis, String key) 
    {
        return jedis.zrange(key, 0, -1);
    }
    
    public Set<String> getSortedSet(Jedis jedis, String key, Double min, Double max)
    {
        return jedis.zrangeByScore(key, min, max);
    }
    
    public void delSortedSet(Jedis jedis, String key)
    {
        jedis.zrem(key);
    }
    
    public void delSortedSet(Jedis jedis, String key, Double start, Double end)
    {
        jedis.zremrangeByScore(key, start, end);
    }
    
    /**
     * 设置数据的到期时间
     * 
     * @param jedis
     * @param key
     * @param value
     * @param seconds
     */
    public void setStringWithExpireTime(Jedis jedis, String key, String value, int seconds)
    {
        jedis.setex(key, seconds, value);
    }
}
