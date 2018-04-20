package com.easyframework.redis.conf;


import org.apache.log4j.Logger;

import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPool;
import redis.clients.jedis.JedisPoolConfig;

public final class RedisHelper{
    
	private static Logger log = Logger.getLogger(RedisHelper.class);
    //Redis服务器IP
/*    private static String ADDR = "192.168.50.129";
    
    //Redis的端口号
    private static int PORT = 6379;
    
    //访问密码
    private static String AUTH = "linzi_422109";
    
    //可用连接实例的最大数目，默认值为8；
    //如果赋值为-1，则表示不限制；如果pool已经分配了maxActive个jedis实例，则此时pool的状态为exhausted(耗尽)。
    private static int MAX_ACTIVE = 1024;
    
    //控制一个pool最多有多少个状态为idle(空闲的)的jedis实例，默认值也是8。
    private static int MAX_IDLE = 200;
    
    //等待可用连接的最大时间，单位毫秒，默认值为-1，表示永不超时。如果超过等待时间，则直接抛出JedisConnectionException；
    private static long MAX_WAIT = 10000;
    
    private static int TIMEOUT = 10000;
    
    //在borrow一个jedis实例时，是否提前进行validate操作；如果为true，则得到的jedis实例均是可用的；
    private static boolean TEST_ON_BORROW = true;*/
    
    private static JedisPool jedisPool = null;
    /**
     * 初始化Redis连接池
     */
    static {
        try {
        	JedisPoolConfig c=new JedisPoolConfig(); 
        	c.setBlockWhenExhausted(true); // 连接耗尽则阻塞
            c.setLifo(ConfigUtils.Lifo); // 后进先出
            c.setMaxIdle(ConfigUtils.Maxidle); // 最大空闲连接数为10
            c.setMinIdle(ConfigUtils.Minidle); // 最小空闲连接数为0
            c.setMaxTotal(ConfigUtils.MaxTotal); // 最大连接数为20
            c.setMaxWaitMillis(ConfigUtils.MaxWaitMillis); // 设置最大等待毫秒数：无限制
            c.setMinEvictableIdleTimeMillis(ConfigUtils.MinEvictableIdleTimeMillis); // 逐出连接的最小空闲时间：30分钟
            c.setTestOnBorrow(ConfigUtils.Borrow); // 获取连接时是否检查连接的有效性：是
            c.setTestWhileIdle(ConfigUtils.WhileIdle); // 空闲时是否检查连接的有效性：是
            jedisPool = new JedisPool(c, ConfigUtils.RedisIP, ConfigUtils.RedisPort);
           // System.out.println("jedisPool:"+jedisPool);
        } catch (Exception e) {
        	log.error("初始化Redis连接池异常："+e.getMessage());
        }
    }
    
    /**
     * 获取Jedis实例
     * @return
     */
    public synchronized static Jedis getJedis() {
        try {
            if (null!= jedisPool) {
            	Jedis resource = jedisPool.getResource();
                	  resource.select(ConfigUtils.DBIndex);
                log.info("\n获取Jedis实例:"+resource);
                return resource;
            }
        } catch (Exception e) {
        	log.error("\n获取Jedis实例异常："+e);
        }
        return null;
    }
    
    /**
     * 释放jedis资源
     * @param jedis
     */
    @SuppressWarnings("deprecation")
	public static void returnResource(Jedis jedis) {
    	//System.out.println("释放jedis资源:"+jedis);
        if ((jedis != null) && jedis.isConnected()) {
            jedisPool.returnResource(jedis);
        }
    }
    
    public static void returnBrokenResource(Jedis jedis) {
    	//System.out.println("释放jedis-returnBrokenResource资源:"+jedis);
        if ((jedis != null) && jedis.isConnected()) {
            jedisPool.returnBrokenResource(jedis);
        }
    }
   
}