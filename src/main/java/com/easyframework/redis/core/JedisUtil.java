package com.easyframework.redis.core;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import javax.script.ScriptEngine;
import javax.script.ScriptEngineManager;
import javax.script.ScriptException;

import org.apache.log4j.Logger;

import redis.clients.jedis.Jedis;
import redis.clients.jedis.ScanParams;
import redis.clients.jedis.ScanResult;
import redis.clients.jedis.SortingParams;
import redis.clients.jedis.exceptions.JedisConnectionException;
import redis.clients.jedis.exceptions.JedisDataException;
import redis.clients.jedis.exceptions.JedisException;

import com.easyframework.redis.conf.ConfigUtils;
import com.easyframework.redis.conf.RedisHelper;




public class JedisUtil{
	
	private static Logger logger = Logger.getLogger(JedisUtil.class);
	
	private static Jedis jedis =RedisHelper.getJedis();
	
	
	/**
	 * 自定义数据存放的数据库DB索引
	 * @param index
	 * @return
	 */
	public static  String selectDBIndex(int index){
		try {
			if(null==jedis){
			 jedis = RedisHelper.getJedis();
			}
			return jedis.select(index);
		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		return null;
	}
	
	/**
	 * 清空对应数据DB索引，库中全部数据
	 * @return
	 */
	public static  String ClearDBData(int index){
		try {
			if(null==jedis){
				 jedis = RedisHelper.getJedis();
				}
			 jedis.select(index);
			return jedis.flushDB();
		} catch (Exception e) {
			// TODO 自动生成的 catch 块
			logger.error("清空对应数据DB索引异常："+e);
		}
		return null;
	}
	
	/**
	 * 清空对应数据库所有索引，库中全部数据
	 * @return
	 */
	public static  String ClearAllDBData(){
		try {
			if(null==jedis){
				 jedis = RedisHelper.getJedis();
				}
			return jedis.flushAll();
		} catch (Exception e) {
			// TODO 自动生成的 catch 块
			logger.error("清空对应数据DB索引异常："+e);
		}
		return null;
	}
	
	 // 键值增加字符
    public Long append(String key, String str) {
    	try {
    		if(null==jedis){
   			 jedis = RedisHelper.getJedis();
   			}
			    return jedis.append(key, str);
		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		return null;
    }
    //判断redis中是否存在key
    public Boolean exists(String key) {
    	try {
    		if(null==jedis){
   			 jedis = RedisHelper.getJedis();
   			}
    		jedis.select(ConfigUtils.DBIndex);
			return jedis.exists(key);
		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		return null;
    }
	
	
	/**
	 * 根据匹配键值key*,获取所有字符串集合
	 * @param pattern
	 * @return
	 */
	public static Set<String> getKeys(String pattern,int index) {
		//Jedis jedis=null;
		boolean broken = false;
		try {
			if(null==jedis){
				 jedis = RedisHelper.getJedis();
				}
			jedis.select(index);
			return jedis.keys(pattern);
		}  catch (JedisException ex) {
			broken = handleJedisException(ex);
			logger.error("根据匹配键值key*,获取所有字符串集合，异常："+ex);
			//returnBrokenResource(jedis);
		} finally {
			//returnResource(jedis);
			closeResource(jedis, broken);
		}
		return null;
	}
	/**
	 * 获取key对应的键string值
	 * @param key
	 * @return
	 */
	public static String get(String key,int index) {
		//Jedis jedis=null;
		boolean broken = false;
		try {
			if(null==jedis){
				 jedis = RedisHelper.getJedis();
				}
			jedis.select(index);
			return jedis.get(key);
		}  catch (JedisException ex) {
			broken = handleJedisException(ex);
			logger.error("get error.", ex);
			//returnBrokenResource(jedis);
		} finally {
			//returnResource(jedis);
			closeResource(jedis, broken);
		}
		return null;
	}
	
	public static boolean set(String key, String value,int index) {
	//	Jedis jedis=null;
		
		boolean broken = false;
		try {
			if(null==jedis){
				 jedis = RedisHelper.getJedis();
				}
			jedis.select(index);
			jedis.set(key, value);
		 	if(ConfigUtils.defaultExpire>0)jedis.expire(key, ConfigUtils.defaultExpire);
		 	//if(ConfigUtils.cacheExpire>0)jedis.expire(key, ConfigUtils.cacheExpire);
			return true;
		} catch (JedisException ex) {
			broken = handleJedisException(ex);
			logger.error("set error.", ex);
			//returnBrokenResource(jedis);
		} finally {
			//returnResource(jedis);
			closeResource(jedis, broken);
		}
		return false;
	}
	/**
	 * 设置一个key的过期时间（单位：秒）
	 * 
	 * @param key
	 *            key值
	 * @param seconds
	 *            多少秒后过期
	 * @return 1：设置了过期时间 0：没有设置过期时间/不能设置过期时间
	 */
	public static long expire(String key, int seconds) {
		//Jedis jedis=null;
		if (key == null || key.equals("")) {
			return 0;
		}
		
		boolean broken = false;
		try {
			if(null==jedis){
				 jedis = RedisHelper.getJedis();
				}
			return jedis.expire(key, seconds);
		} catch (JedisException ex) {
			broken = handleJedisException(ex);
				logger.error("EXPIRE error[key=" + key + " seconds=" + seconds+ "]" + ex.getMessage(), ex);
				//returnBrokenResource(jedis);
		} finally {
			//returnResource(jedis);
			closeResource(jedis, broken);
		}
		return 0;
	}
	
	/**
	 * 设置一个key的过期时间（单位：秒）
	 * 
	 * @param key
	 *            key值
	 * @param seconds
	 *            多少秒后过期
	 * @return 1：设置了过期时间 0：没有设置过期时间/不能设置过期时间
	 */
	public static long expire(String key, int seconds,int index) {
		//Jedis jedis=null;
		if (key == null || key.equals("")) {
			return 0;
		}
		
		boolean broken = false;
		try {
			if(null==jedis){
				 jedis = RedisHelper.getJedis();
				}
				jedis.select(index);
				return jedis.expire(key, seconds);
		}catch (JedisException ex) {
			broken = handleJedisException(ex);
				logger.error("EXPIRE error[key=" + key + " seconds=" + seconds+ "]" + ex.getMessage(), ex);
			//	returnBrokenResource(jedis);
		} finally {
			//returnResource(jedis);
			closeResource(jedis, broken);
		}
		return 0;
	}
	/**
	 * 设置一个key在某个时间点过期
	 * 
	 * @param key
	 *            key值
	 * @param unixTimestamp
	 *            unix时间戳，从1970-01-01 00:00:00开始到现在的秒数
	 * @return 1：设置了过期时间 0：没有设置过期时间/不能设置过期时间
	 */
	public static long expireAt(String key, int unixTimestamp) {
		if (key == null || key.equals("")) {
			return 0;
		}
		//Jedis jedis=null;
		boolean broken = false;
		try {
			if(null==jedis){
				 jedis = RedisHelper.getJedis();
				}
			 return jedis.expireAt(key, unixTimestamp);
		}catch (JedisException ex) {
			broken = handleJedisException(ex);
			logger.error("EXPIRE error[key=" + key + " unixTimestamp="+ unixTimestamp + "]" + ex.getMessage(), ex);
			//returnBrokenResource(jedis);
		} finally {
			//returnResource(jedis);
			closeResource(jedis, broken);
		}
		return 0;
	}
	
	/**
	 * 设置一个key在某个时间点过期
	 * 
	 * @param key
	 *            key值
	 * @param unixTimestamp
	 *            unix时间戳，从1970-01-01 00:00:00开始到现在的秒数
	 * @return 1：设置了过期时间 0：没有设置过期时间/不能设置过期时间
	 */
	public static long expireAt(String key, int unixTimestamp,int index) {
		if (key == null || key.equals("")) {
			return 0;
		}
		//Jedis jedis=null;
		boolean broken = false;
		try {
			if(null==jedis){
				 jedis = RedisHelper.getJedis();
				}
			  jedis.select(index);
			 return jedis.expireAt(key, unixTimestamp);
		}catch (JedisException ex) {
			broken = handleJedisException(ex);
			logger.error("EXPIRE error[key=" + key + " unixTimestamp="+ unixTimestamp + "]" + ex.getMessage(), ex);
			//returnBrokenResource(jedis);
		} finally {
			//returnResource(jedis);
			closeResource(jedis, broken);
		}
		return 0;
	}
	/**
	 *  截断一个List
	 * 
	 * @param key
	 *            列表key
	 * @param start
	 *            开始位置 从0开始
	 * @param end
	 *            结束位置
	 * @return 状态码
	 */
	public static String trimList(String key, long start, long end) {
		if (key == null || key.equals("")) {
			return "-";
		}
		//Jedis jedis=null;
		boolean broken = false;
		try {
			if(null==jedis){
				 jedis = RedisHelper.getJedis();
				}
			jedis.select(ConfigUtils.DBIndex);
			 return jedis.ltrim(key, start, end);
		}catch (JedisException ex) {
			broken = handleJedisException(ex);
			logger.error("LTRIM 出错[key=" + key + " start=" + start + " end="+ end + "]" + ex.getMessage(), ex);
			//returnBrokenResource(jedis);
		} finally {
			//returnResource(jedis);
			closeResource(jedis, broken);
		}
		return "-";
	}
	
	
	/**
	 * 检查Set长度
	 * @param key
	 * @return
	 */
	public static long countSet(String key) {
		if (key == null) {
			return 0;
		}
		//Jedis jedis=null;
		boolean broken = false;
		try {
			if(null==jedis){
				 jedis = RedisHelper.getJedis();
				}
			jedis.select(ConfigUtils.DBIndex);
			 return jedis.scard(key);
		} catch (JedisException ex) {
			broken = handleJedisException(ex);
			logger.error("countSet error.", ex);
			//returnBrokenResource(jedis);
		} finally {
			//returnResource(jedis);
			closeResource(jedis, broken);
		}
		return 0;
	}
	
	
	/**
	 * 添加到Set中（同时设置过期时间）
	 * 
	 * @param key
	 *            key值
	 * @param seconds
	 *            过期时间 单位s
	 * @param value
	 * @return
	 */
	public static boolean addSet(String key, int seconds, String... value) {
		boolean result = addSet(key, value);
		if (result) {
		long i = expire(key, seconds);
		return i == 1;
		}
		return false;
	}
	
	
	
	/**
	 * 添加到Set中
	 * @param key
	 * @param value
	 * @return
	 */
	public static boolean addSet(String key, String... value) {
		if (key == null || value == null) {
			return false;
		}
		//Jedis jedis=null;
		boolean broken = false;
		try {
			if(null==jedis){
				 jedis = RedisHelper.getJedis();
				}
			jedis.select(ConfigUtils.DBIndex);
			 jedis.sadd(key, value);
			 return true;
		} catch (JedisException ex) {
			broken = handleJedisException(ex);
			logger.error("setList error.", ex);
			//returnBrokenResource(jedis);
		} finally {
			//returnResource(jedis);
			closeResource(jedis, broken);
		}
		return false;
	}
	
	
	/**
	 * 判断值是否包含在set中
	 * @param key
	 * @param value
	 * @return
	 */
	public static boolean containsInSet(String key, String value) {
		if (key == null || value == null) {
			return false;
		}
		//Jedis jedis=null;
		boolean broken = false;
		try {
			if(null==jedis){
				 jedis = RedisHelper.getJedis();
				}
			jedis.select(ConfigUtils.DBIndex);
			 return jedis.sismember(key, value);
		} catch (JedisException ex) {
			broken = handleJedisException(ex);
			logger.error("setList error.", ex);
			//returnBrokenResource(jedis);
		} finally {
			//returnResource(jedis);
			closeResource(jedis, broken);
		}
		return false;
	}
	
	
	/**
	 * 获取Set
	 * @param key
	 * @return
	 */
	public static Set<String> getSet(String key) {
		//Jedis jedis=null;
		boolean broken = false;
		try {
			if(null==jedis){
				 jedis = RedisHelper.getJedis();
				}
			jedis.select(ConfigUtils.DBIndex);
			return jedis.smembers(key);
		} catch (JedisException ex) {
			broken = handleJedisException(ex);
			logger.error("getList error.", ex);
			//returnBrokenResource(jedis);
		} finally {
			//returnResource(jedis);
			closeResource(jedis, broken);
		}
		return null;
	}
	
	
	/**
	 * 从set中删除value
	 * @param key
	 * @param value
	 * @return
	 */
	public static boolean removeSetValue(String key, String... value) {
		//Jedis jedis=null;
		boolean broken = false;
		try {
			if(null==jedis){
				 jedis = RedisHelper.getJedis();
				}
			jedis.select(ConfigUtils.DBIndex);
			jedis.srem(key, value);
			return true;
		} catch (JedisException ex) {
			broken = handleJedisException(ex);
			logger.error("getList error.", ex);
			//returnBrokenResource(jedis);
		} finally {
			//returnResource(jedis);
			closeResource(jedis, broken);
		}
		return false;
	}
	
	
	/**
	 * 从list中删除value 默认count 1
	 * @param key
	 * @param values
	 * @return
	 */
	public static int removeListValue(String key, List<String> values) {
		return removeListValue(key, 1, values);
	}
	
	/**
	 * 从list中删除value
	 * @param key
	 * @param count
	 * @param values
	 * @return
	 */
	public static int removeListValue(String key, long count,
			List<String> values) {
			int result = 0;
			if (values != null && values.size() > 0) {
				for (String value : values) {
					if (removeListValue(key, count, value)) {
						result++;
					}
				}
			}
			return result;
	}
	
	
	
	
	/**
	 * 从list中删除value
	 * @param key
	 * @param count要删除个数
	 * @param value
	 * @return
	 */
	public static boolean removeListValue(String key, long count, String value) {
		//Jedis jedis=null;
		boolean broken = false;
		try {
			if(null==jedis){
				 jedis = RedisHelper.getJedis();
				}
			jedis.select(ConfigUtils.DBIndex);
			jedis.lrem(key, count, value);
			return true;
		} catch (JedisException ex) {
			broken = handleJedisException(ex);
			logger.error("getList error.", ex);
			//returnBrokenResource(jedis);
		} finally {
			//returnResource(jedis);
			closeResource(jedis, broken);
		}
		return false;
	}
	
	
	
	/**
	 *  * 截取List
	 * 
	 * @param key
	 * @param start
	 *            起始位置
	 * @param end
	 *            结束位置
	 * @return
	 */
	public static List<String> rangeList(String key, long start, long end) {
		if (key == null || key.equals("")) {
			return null;
		}
	//	Jedis jedis=null;
		boolean broken = false;
		try {
			if(null==jedis){
				 jedis = RedisHelper.getJedis();
				}
			jedis.select(ConfigUtils.DBIndex);
			return jedis.lrange(key, start, end);
		}catch (JedisException ex) {
			broken = handleJedisException(ex);
			logger.error("rangeList 出错[key=" + key + " start=" + start+ " end=" + end + "]" + ex.getMessage(), ex);
			//returnBrokenResource(jedis);
		} finally {
			//returnResource(jedis);
			closeResource(jedis, broken);
		}
		return null;
	}
	
	/**
	 * 检查List长度
	 * @param key
	 * @return
	 */
	public static long countList(String key) {
		if (key == null) {
			return 0;
		}
		//Jedis jedis=null;
		boolean broken = false;
		try {
			if(null==jedis){
				 jedis = RedisHelper.getJedis();
				}
			jedis.select(ConfigUtils.DBIndex);
			return jedis.llen(key);
		} catch (JedisException ex) {
			broken = handleJedisException(ex);
			logger.error("countList error.", ex);
			//returnBrokenResource(jedis);
		} finally {
			//returnResource(jedis);
			closeResource(jedis, broken);
		}
		return 0;
	}
	
	
	/**
	 * 添加到List中（同时设置过期时间）
	 * @param key
	 * @param seconds过期时间 单位s
	 * @param value
	 * @return
	 */
	public static boolean addList(String key, int seconds, String... value) {
		boolean result = addList(key, value);
		if (result) {
		long i = expire(key, seconds);
		return i == 1;
		}
		return false;
	}
	
	/**
	 * 添加到List
	 * @param key
	 * @param value
	 * @return
	 */
	public static boolean addList(String key, String... value) {
		if (key == null || value == null) {
			return false;
		}
		//Jedis jedis=null;
		boolean broken = false;
		try {
			if(null==jedis){
				 jedis = RedisHelper.getJedis();
				}
			jedis.select(ConfigUtils.DBIndex);
			jedis.lpush(key, value);
			return true;
		} catch (JedisException ex) {
			broken = handleJedisException(ex);
			logger.error("setList error.", ex);
			//returnBrokenResource(jedis);
		} finally {
			//returnResource(jedis);
			closeResource(jedis, broken);
		}
		return false;
	}
	
	
	
	/**
	 * 添加到List(只新增)
	 * @param key
	 * @param list
	 * @return
	 */
	public static boolean addList(String key, List<String> list) {
		if (key == null || list == null || list.size() == 0) {
			return false;
		}
		for (String value : list) {
			addList(key, value);
		}
		return true;
	}
	
	
	/**
	 * 获取List
	 * @param key
	 * @return
	 */
	public static List<String> getList(String key) {
		//Jedis jedis=null;
		boolean broken = false;
		try {
			if(null==jedis){
				 jedis = RedisHelper.getJedis();
				}
			jedis.select(ConfigUtils.DBIndex);
			return jedis.lrange(key, 0, -1);
		} catch (JedisException ex) {
			broken = handleJedisException(ex);
			logger.error("getList error.", ex);
			//returnBrokenResource(jedis);
		} finally {
			//returnResource(jedis);
			closeResource(jedis, broken);
		}
		return null;
	}
	
	/**
	 * * 设置HashSet对象
	 * 
	 * @param domain
	 *            域名
	 * @param key
	 *            键值
	 * @param value
	 *  		 Json String or String value
	 * @return
	 */
	public static boolean setHSet(String domain, String key, String value) {
		if (value == null) return false;
		//Jedis jedis=null;
		boolean broken = false;
		try {
			if(null==jedis){
				 jedis = RedisHelper.getJedis();
				}
			jedis.select(ConfigUtils.DBIndex);
			jedis.hset(domain, key, value);
			if(ConfigUtils.defaultExpire>0)jedis.expire(key, ConfigUtils.defaultExpire);
			return true;
		}catch (JedisException ex) {
			broken = handleJedisException(ex);
			logger.error("setHSet error.", ex);
			//returnBrokenResource(jedis);
		} finally {
			//returnResource(jedis);
			closeResource(jedis, broken);
		}
		return false;
	}
	
	
	/**
	 *  * 获得HashSet对象
	 * 
	 * @param domain
	 *            域名
	 * @param key
	 *            键值
	 * @return Json String or String value
	 */
	public static String getHSet(String domain, String key) {
		//Jedis jedis=null;
		boolean broken = false;
		try {
			if(null==jedis){
				 jedis = RedisHelper.getJedis();
				}
			jedis.select(ConfigUtils.DBIndex);
			return jedis.hget(domain, key);
		}catch (JedisException ex) {
			broken = handleJedisException(ex);
			logger.error("getHSet error.", ex);
		//	returnBrokenResource(jedis);
		} finally {
			//returnResource(jedis);
			closeResource(jedis, broken);
		}
		return null;
	}
	
	
	/**
	 * * 删除HashSet对象
	 * 
	 * @param domain
	 *            域名
	 * @param key
	 *            键值
	 * @return 删除的记录数
	 */
	public static long delHSet(String domain, String key) {
		long count = 0;
		//Jedis jedis=null;
		boolean broken = false;
		try {
			if(null==jedis){
				 jedis = RedisHelper.getJedis();
				}
			jedis.select(ConfigUtils.DBIndex);
			count = jedis.hdel(domain, key);
		} catch (JedisException ex) {
			broken = handleJedisException(ex);
			logger.error("delHSet error.", ex);
			//returnBrokenResource(jedis);
		} finally {
			//returnResource(jedis);
			closeResource(jedis, broken);
		}
		return count;
	}
	
	
	/**
	 * * 删除HashSet对象
	 * 
	 * @param domain
	 *            域名
	 * @param key
	 *            键值
	 * @return 删除的记录数
	 */
	public static long delHSet(String domain, String... key) {
		long count = 0;
		//Jedis jedis=null;
		boolean broken = false;
		try {
			if(null==jedis){
				 jedis = RedisHelper.getJedis();
				}
			jedis.select(ConfigUtils.DBIndex);
			count = jedis.hdel(domain, key);
		}catch (JedisException ex) {
			broken = handleJedisException(ex);
			logger.error("delHSet error.", ex);
			//returnBrokenResource(jedis);
		} finally {
			//returnResource(jedis);
			closeResource(jedis, broken);
		}
		return count;
	}
	
	
	/**
	 * * 判断key是否存在
	 * 
	 * @param domain
	 *            域名
	 * @param key
	 *            键值
	 * @return
	 */
	public static boolean existsHSet(String domain, String key) {
		boolean isExist = false;
		//Jedis jedis=null;
		boolean broken = false;
		try {
			if(null==jedis){
				 jedis = RedisHelper.getJedis();
				}
			jedis.select(ConfigUtils.DBIndex);
			isExist = jedis.hexists(domain, key);
		} catch (JedisException ex) {
			broken = handleJedisException(ex);
			logger.error("existsHSet error.", ex);
			//returnBrokenResource(jedis);
		} finally {
			//returnResource(jedis);
			closeResource(jedis, broken);
		}
		return isExist;
	}
	
	/**
	 * 全局扫描hset
	 * 
	 * @param match
	 *            field匹配模式
	 * @return
	 */
	public static List<Map.Entry<String, String>> scanHSet(String domain,String match) {
		//Jedis jedis=null;
		boolean broken = false;
			try {
				int cursor = 0;
				if(null==jedis){
					 jedis = RedisHelper.getJedis();
					}
			ScanParams scanParams = new ScanParams();
						scanParams.match(match);
			ScanResult<Map.Entry<String, String>> scanResult;
			List<Map.Entry<String, String>> list = new ArrayList<Map.Entry<String, String>>();
			do {
					jedis.select(ConfigUtils.DBIndex);
					scanResult = jedis.hscan(domain, String.valueOf(cursor),scanParams);
					list.addAll(scanResult.getResult());
					cursor = Integer.parseInt(scanResult.getStringCursor());
			} while (cursor > 0);
				return list;
			}catch (JedisException ex) {
				broken = handleJedisException(ex);
				logger.error("scanHSet error.", ex);
				//returnBrokenResource(jedis);
			} finally {
				//returnResource(jedis);
				closeResource(jedis, broken);
			}
			return null;
	}
	
	
	
	/**
	 *  * 全局扫描hset
	 * 
	 * @param match
	 *            field匹配模式
	 * @return
	 */
	public static Set<String>  scan(String match) {
		//Jedis jedis=null;
		boolean broken = false;
			try {
				int cursor = 0;
				if(null==jedis){
					 jedis = RedisHelper.getJedis();
					}
			ScanParams scanParams = new ScanParams();
						scanParams.match(match);
			ScanResult<String> scanResult;
			Set<String> retSet  = new HashSet<String>();
			do {
				jedis.select(ConfigUtils.DBIndex);
						scanResult = jedis.scan(String.valueOf(cursor),scanParams);
						retSet.addAll(scanResult.getResult());
						cursor = Integer.parseInt(scanResult.getStringCursor());
			} while (cursor > 0);
				return retSet;
			}catch (JedisException ex) {
				broken = handleJedisException(ex);
				logger.error("scanHSet error.", ex);
				//returnBrokenResource(jedis);
			} finally {
				//returnResource(jedis);
				closeResource(jedis, broken);
			}
				return null;
	}
	
	
	/**
	 * 返回 domain 指定的哈希集中所有字段的value值
	 * @param domain
	 * @return
	 */
	public static List<String> hvals(String domain) {
		List<String> retList = null;
		//Jedis jedis=null;
		boolean broken = false;
		try {
			if(null==jedis){
				 jedis = RedisHelper.getJedis();
				}
			jedis.select(ConfigUtils.DBIndex);
			retList = jedis.hvals(domain);
		}catch (JedisException ex) {
			broken = handleJedisException(ex);
			logger.error("hvals error.", ex);
			//returnBrokenResource(jedis);
		} finally {
			//returnResource(jedis);
			closeResource(jedis, broken);
		}
		return retList;
	}
	
	
	/**
	 * 返回 domain 指定的哈希集中所有字段的key值
	 * @param domain
	 * @return
	 */
	public static Set<String> hkeys(String domain) {
		Set<String> retList = null;
		//Jedis jedis=null;
		boolean broken = false;
		try {
			if(null==jedis){
				 jedis = RedisHelper.getJedis();
				}
			jedis.select(ConfigUtils.DBIndex);
			retList = jedis.hkeys(domain);
		}catch (JedisException ex) {
			broken = handleJedisException(ex);
			logger.error("hkeys error.", ex);
			//returnBrokenResource(jedis);
		} finally {
			//returnResource(jedis);
			closeResource(jedis, broken);
		}
			return retList;
	}
	
	
	/**
	 * 返回 domain 指定的哈希key值总数
	 * 
	 * @param domain
	 * @return
	 */
	public static long lenHset(String domain) {
		long retList = 0;
		//Jedis jedis=null;
		boolean broken = false;
		try {
			if(null==jedis){
				 jedis = RedisHelper.getJedis();
				}
			jedis.select(ConfigUtils.DBIndex);
			retList = jedis.hlen(domain);
		}catch (JedisException ex) {
			broken = handleJedisException(ex);
			logger.error("hkeys error.", ex);
			//returnBrokenResource(jedis);
		} finally {
			//returnResource(jedis);
			closeResource(jedis, broken);
		}
		return retList;
	}
	/**
	 * 设置排序集合
	 * 
	 * @param key
	 * @param score
	 * @param value
	 * @return
	 */
	public static boolean setSortedSet(String key, long score, String value) {
		//Jedis jedis=null;
		boolean broken = false;
		try {
			if(null==jedis){
				 jedis = RedisHelper.getJedis();
				}
			jedis.select(ConfigUtils.DBIndex);
			jedis.zadd(key, score, value);
			return true;
		}catch (JedisException ex) {
			broken = handleJedisException(ex);
			logger.error("setSortedSet error.", ex);
			//returnBrokenResource(jedis);
		} finally {
			//returnResource(jedis);
			closeResource(jedis, broken);
		}
			return false;
	}
	/**
	 * 参数排序集合
	 * 
	 * @param key
	 * @param score
	 * @param value
	 * @return
	 */
	public static List<String> getSortedSetByParams(String pattn,int start,int count,boolean orderByDesc ) {
		//Jedis jedis=null;
		boolean broken = false;
		try {
			if(null==jedis){
				 jedis = RedisHelper.getJedis();
				}
			SortingParams sp=new SortingParams();
			if(orderByDesc){
				sp.desc();
			}else{
				sp.asc();
			}
			if(start>0&&count>0){
				sp.limit(start, count);
			}
			sp.get(pattn);
			jedis.select(ConfigUtils.DBIndex);
			return jedis.sort(pattn, sp);
		}catch (JedisException ex) {
			broken = handleJedisException(ex);
			logger.error("getSortedSet error.", ex);
			//returnBrokenResource(jedis);
		} finally {
			//returnResource(jedis);
			closeResource(jedis, broken);
		}
			return null;
	}
	/**
	 * 获得排序集合
	 * 
	 * @param key
	 * @param startScore
	 * @param endScore
	 * @param orderByDesc
	 * @return
	 */
	public static Set<String> getSoredSet(String key, long startScore,long endScore, boolean orderByDesc) {
		//Jedis jedis=null;
		boolean broken = false;
		try {
			if(null==jedis){
				 jedis = RedisHelper.getJedis();
				}
			jedis.select(ConfigUtils.DBIndex);
			if (orderByDesc) {
				return jedis.zrevrangeByScore(key, endScore, startScore);
			} else {
				return jedis.zrangeByScore(key, startScore, endScore);
			}
		}catch (JedisException ex) {
			broken = handleJedisException(ex);
			logger.error("getSoredSet error.", ex);
			//returnBrokenResource(jedis);
		} finally {
			//returnResource(jedis);
			closeResource(jedis, broken);
		}
		return null;
	}
	/**
	 * 计算排序长度
	 * 
	 * @param key
	 * @param startScore
	 * @param endScore
	 * @return
	 */
	public static long countSoredSet(String key, long startScore, long endScore) {
		//Jedis jedis=null;
		boolean broken = false;
		try {
			if(null==jedis){
				 jedis = RedisHelper.getJedis();
				}
			jedis.select(ConfigUtils.DBIndex);
			Long count = jedis.zcount(key, startScore, endScore);
			return count == null ? 0L : count;
		}catch (JedisException ex) {
			broken = handleJedisException(ex);
			logger.error("countSoredSet error.", ex);
			//returnBrokenResource(jedis);
		} finally {
			//returnResource(jedis);
			closeResource(jedis, broken);
		}
		return 0L;
	}
	
	/**
	 * 删除排序集合
	 * 
	 * @param key
	 * @param value
	 * @return
	 */
	public static boolean delSortedSet(String key, String value) {
		//Jedis jedis=null;
		boolean broken = false;
		try {
			if(null==jedis){
				 jedis = RedisHelper.getJedis();
				}
			jedis.select(ConfigUtils.DBIndex);
			long count = jedis.zrem(key, value);
			return count > 0;
		}catch (JedisException ex) {
			broken = handleJedisException(ex);
			logger.error("delSortedSet error.", ex);
			//returnBrokenResource(jedis);
		} finally {
			//returnResource(jedis);
			closeResource(jedis, broken);
		}
		return false;
	}
	
	/**
	 * 获得排序集合
	 * 
	 * @param key
	 * @param startRange
	 * @param endRange
	 * @param orderByDesc
	 * @return
	 */
	public static Set<String> getSoredSetByRange(String key, int startRange,int endRange, boolean orderByDesc) {
		//Jedis jedis=null;
		boolean broken = false;
		try {
			if(null==jedis){
				 jedis = RedisHelper.getJedis();
				}
			jedis.select(ConfigUtils.DBIndex);
			if (orderByDesc) {
				return jedis.zrevrange(key, startRange, endRange);
			} else {
				return jedis.zrange(key, startRange, endRange);
			}
		}catch (JedisException ex) {
			broken = handleJedisException(ex);
			logger.error("getSoredSetByRange error.", ex);
			//returnBrokenResource(jedis);
		} finally {
			//returnResource(jedis);
			closeResource(jedis, broken);
		}
		return null;
	}
	/**
	 * 获得排序打分
	 * 
	 * @param key
	 * @return
	 */
	public static Double getScore(String key, String member) {
		//Jedis jedis=null;
		boolean broken = false;
		try {
			if(null==jedis){
				 jedis = RedisHelper.getJedis();
				}
			jedis.select(ConfigUtils.DBIndex);
			return jedis.zscore(key, member);
		}catch (JedisException ex) {
			broken = handleJedisException(ex);
			logger.error("getSoredSet error.", ex);
			//returnBrokenResource(jedis);
		} finally {
			//returnResource(jedis);
			closeResource(jedis, broken);
		}
		return null;
	}
	
	public static boolean sett(String key, String value, int second) {
		//Jedis jedis=null;
		boolean broken = false;
		try {
			if(null==jedis){
				 jedis = RedisHelper.getJedis();
				}
			jedis.select(ConfigUtils.DBIndex);
			jedis.setex(key, second, value);
			if(ConfigUtils.defaultExpire>0)jedis.expire(key, ConfigUtils.defaultExpire);
			return true;
		} catch (JedisException ex) {
			broken = handleJedisException(ex);
			logger.error("set error.", ex);
			//returnBrokenResource(jedis);
		} finally {
			//returnResource(jedis);
			closeResource(jedis, broken);
		}
		return false;
	}
	
	//键、值都以字节存入
	public static boolean _set(byte[] key, byte[] value,String type) {
		//Jedis jedis=null;
		boolean broken = false;
		try {
			if(null==jedis){
				 jedis = RedisHelper.getJedis();
				}
			if("clazz".equals(type)){
				jedis.select(ConfigUtils.ClazzDBIndex);
			}else
			if("list".equals(type)){
				jedis.select(ConfigUtils.ListDBIndex);
			}else
			if("table".equals(type)){
				jedis.select(ConfigUtils.TableDBIndex);
			}else{
				jedis.select(ConfigUtils.DBIndex);
			}
			
			jedis.set(key, value);
			
			if("clazz".equals(type)){
				if(ConfigUtils.clazzExpire>0)jedis.expire(key, ConfigUtils.clazzExpire);
			}else
			if("list".equals(type)){
				if(ConfigUtils.listExpire>0)jedis.expire(key, ConfigUtils.listExpire);
			}else
			if("table".equals(type)){
				if(ConfigUtils.tableExpire>0)jedis.expire(key, ConfigUtils.tableExpire);
			}else{
				if(ConfigUtils.defaultExpire>0)jedis.expire(key, ConfigUtils.defaultExpire);
			}
			
			if(ConfigUtils.isDebug){
				logger.info("\n-----------------------------------------------------------------\n" +
								"信息 key:"+key+",value:"+jedis.get(key)+"，保存成功.\n" +
						    "-----------------------------------------------------------------\n");
			}
			return true;
		} catch (JedisException ex) {
			broken = handleJedisException(ex);
			logger.error("set error.", ex);
			//returnBrokenResource(jedis);
		} finally {
			//returnResource(jedis);
			closeResource(jedis, broken);
		}
		return false;
	}
	
	
	/**
	 * (jedis != null) && jedis.isConnected()
	 * @param key
	 * @param value
	 * @return
	 */
	public static boolean _set(String key, String value,String type) {
		//Jedis jedis=null;
		boolean broken = false;
		try {
			if(null==jedis){
				 jedis = RedisHelper.getJedis();
				}
			if("clazz".equals(type)){
				jedis.select(ConfigUtils.ClazzDBIndex);
			}else
			if("list".equals(type)){
				jedis.select(ConfigUtils.ListDBIndex);
			}else
			if("table".equals(type)){
				jedis.select(ConfigUtils.TableDBIndex);
			}else{
				jedis.select(ConfigUtils.DBIndex);
			}
			jedis.set(key, value);
			
			if("clazz".equals(type)){
				if(ConfigUtils.clazzExpire>0)jedis.expire(key, ConfigUtils.clazzExpire);
			}else
			if("list".equals(type)){
				if(ConfigUtils.listExpire>0)jedis.expire(key, ConfigUtils.listExpire);
			}else
			if("table".equals(type)){
				if(ConfigUtils.tableExpire>0)jedis.expire(key, ConfigUtils.tableExpire);
			}else{
				if(ConfigUtils.defaultExpire>0)jedis.expire(key, ConfigUtils.defaultExpire);
			}
			
			
			if(ConfigUtils.isDebug){
				logger.info("\n-------------------------db:"+jedis.getDB()+"------------------------------------\n" +
								"信息 key:"+key+",value:"+jedis.get(key)+"，保存成功.\n" +
						    "-----------------------------------------------------------------\n");
			}
			return true;
		} catch (JedisException ex) {
			broken = handleJedisException(ex);
			logger.error("set error.", ex);
			//returnBrokenResource(jedis);
		} finally {
			//returnResource(jedis);
			closeResource(jedis, broken);
		}
		return false;
	}
	
	public static String get(String key, String defaultValue) {
		//Jedis jedis=null;
		boolean broken = false;
		try {
			if(null==jedis){
				 jedis = RedisHelper.getJedis();
				}
			jedis.select(ConfigUtils.DBIndex);
			return jedis.get(key) == null ? defaultValue : jedis.get(key);
		}catch (JedisException ex) {
			broken = handleJedisException(ex);
			logger.error("get error.", ex);
			//returnBrokenResource(jedis);
		} finally {
			//returnResource(jedis);
			closeResource(jedis, broken);
		}
		return defaultValue;
	}
	
	
	/**
	 * 获取key对应的键字节值
	 * @param key
	 * @return
	 */
	public static byte[] _get(byte[] key,String type) {
		//Jedis jedis=null;
		boolean broken = false;
		try {
			if(null==jedis){
				 jedis = RedisHelper.getJedis();
				}
			if("clazz".equals(type)){
				jedis.select(ConfigUtils.ClazzDBIndex);
			}else
			if("list".equals(type)){
				jedis.select(ConfigUtils.ListDBIndex);
			}else
			if("table".equals(type)){
				jedis.select(ConfigUtils.TableDBIndex);
			}else{
				jedis.select(ConfigUtils.DBIndex);
			}
			return jedis.get(key);
		}catch (JedisException ex) {
			broken = handleJedisException(ex);
			logger.error("获取key对应的键string值，异常："+ ex);
			//returnBrokenResource(jedis);
		} finally {
			//returnResource(jedis);
			closeResource(jedis, broken);
		}
		return null;
	}
	
	/**
	 * 获取key对应的键string值
	 * @param key
	 * @return
	 */
	public static String _get(String key,String type) {
		//Jedis jedis=null;
		boolean broken = false;
		String result=null;
		try {
			if(null==jedis){
				 jedis = RedisHelper.getJedis();
				}
			if("clazz".equals(type)){
				jedis.select(ConfigUtils.ClazzDBIndex);
			}else
			if("list".equals(type)){
				jedis.select(ConfigUtils.ListDBIndex);
			}else
			if("table".equals(type)){
				jedis.select(ConfigUtils.TableDBIndex);
			}else{
				jedis.select(ConfigUtils.DBIndex);
			}
			result= jedis.get(key);
		}catch (JedisException ex) {
			broken = handleJedisException(ex);
			logger.error("获取key对应的键string值，异常："+ ex);
			//returnBrokenResource(jedis);
		} finally {
			//returnResource(jedis);
			closeResource(jedis, broken);
		}
		return result;
	}
	/**
	 * 获取String存储表记录大小
	 * @param pattern:左侧模糊匹配
	 * @return
	 */
	public static int getSize(String pattern) {
		boolean broken = false;
		try {
			if(null==jedis){
				 jedis = RedisHelper.getJedis();
				}
			jedis.select(ConfigUtils.DBIndex);
			Set<String> s=jedis.keys(pattern);
			if(s!=null){
				return s.size();
			}else{
				return 0;
			}
		}catch (JedisException ex) {
			broken = handleJedisException(ex);
			logger.error("获取String存储表记录大小，异常："+ ex);
		} finally {
			closeResource(jedis, broken);
		}
		return 0;
	}
	
	/**
	 * 过滤重复id,之后为对象个数
	 * 对象属性过滤处理(person:xx0001:id)
	 * @param pattern
	 * @return
	 */
	public static Set<String> getIds(Set<String> sset) {
		try {
			Set<String> ids=new HashSet<String>();
			for(String s:sset){
				String []tags=s.split(":");
				//ids.add(tags[1]+":"+tags[2]);
				ids.add(tags[1]);//返回对象属性：xx0001:id==>xx0001
			}
			return ids;
		} catch (Exception ex) {
			logger.error("过滤重复id,之后为对象个数，异常："+ ex);
		} 
		return null;
	}
	/**
	 * 过滤重复id,之后为对象个数
	 * @param pattern
	 * @return
	 */
	public static Set<String> getIdx(Set<String> sset,List<String> columns) {
		try {
			Set<String> ids=new HashSet<String>();
			for(String s:sset){
				String []tags=s.split(":");
				ids.add(tags[2]);
			}
			return ids;
		} catch (Exception ex) {
			logger.error("过滤重复id,之后为对象个数，异常："+ ex);
		} 
		return null;
	}
	/**
	 * 根据匹配键值key*,获取所有字符串集合
	 * @param pattern
	 * @return
	 */
	public static Set<String> getKeys(String pattern) {
		//Jedis jedis=null;
		boolean broken = false;
		try {
			if(null==jedis){
				 jedis = RedisHelper.getJedis();
				}
			//boolean isExists=jedis.exists(pattern);//pattern用全名
			//Set<String> keys = jedis.keys(pattern);
			return jedis.keys(pattern);
		}catch (JedisException ex) {
			broken = handleJedisException(ex);
			logger.error(" 根据匹配键值key*,获取所有字符串集合，异常："+ ex);
		} finally {
			closeResource(jedis, broken);
		}
		return null;
	}
	
	public static Set<String> _getKeys(String pattern,String type) {
		//Jedis jedis=null;
		boolean broken = false;
		try {
			if(null==jedis){
				 jedis = RedisHelper.getJedis();
				}
			if("clazz".equals(type)){
				jedis.select(ConfigUtils.ClazzDBIndex);
			}else
			if("list".equals(type)){
				jedis.select(ConfigUtils.ListDBIndex);
			}else
			if("table".equals(type)){
				jedis.select(ConfigUtils.TableDBIndex);
			}else{
				jedis.select(ConfigUtils.DBIndex);
			}
			return jedis.keys(pattern);
		}catch (JedisException ex) {
			broken = handleJedisException(ex);
			logger.error(" 根据匹配键值key*,获取所有字符串集合，异常："+ ex);
		} finally {
			closeResource(jedis, broken);
		}
		return null;
	}
	
	/**
	 * 删除key对应的键值
	 * @param key
	 * @return
	 */
	public static boolean del(String key) {
		//Jedis jedis=null;
		boolean broken = false;
		try {
			if(null==jedis){
				 jedis = RedisHelper.getJedis();
				}
			jedis.select(ConfigUtils.DBIndex);
			jedis.del(key);
			return true;
		}catch (JedisException ex) {
			broken = handleJedisException(ex);
			logger.error(" 删除key对应的键值，异常："+ ex);
		} finally {
			closeResource(jedis, broken);
		}
		return false;
	}
	
	
	/**
	 * 删除key对应的键值
	 * @param key
	 * @return
	 */
	public static boolean del(String key,String type) {
		//Jedis jedis=null;
		boolean broken = false;
		try {
			if(null==jedis){
				 jedis = RedisHelper.getJedis();
				}
			if("clazz".equals(type)){
				jedis.select(ConfigUtils.ClazzDBIndex);
			}else
			if("list".equals(type)){
				jedis.select(ConfigUtils.ListDBIndex);
			}else
			if("table".equals(type)){
				jedis.select(ConfigUtils.TableDBIndex);
			}else{
				jedis.select(ConfigUtils.DBIndex);
			}
			jedis.del(key);
			return true;
		}catch (JedisException ex) {
			broken = handleJedisException(ex);
			logger.error(" 删除key对应的键值，异常："+ ex);
			//returnBrokenResource(jedis);
		} finally {
			//returnResource(jedis);
			closeResource(jedis, broken);
		}
		return false;
	}
	
	
	
	/**
	 * 批量删除key对应的键值
	 * @param key
	 * @return
	 */
	public static boolean del(String... key) {
		boolean broken = false;
		try {
			if(null==jedis){
				 jedis = RedisHelper.getJedis();
				}
			jedis.select(ConfigUtils.DBIndex);
			jedis.del(key);
			return true;
		}catch (JedisException ex) {
			broken = handleJedisException(ex);
			logger.error("批量删除key对应的键值，异常："+ ex);
			//returnBrokenResource(jedis);
		} finally {
			closeResource(jedis, broken);
		}
		return false;
	}
	
	
	/**
	 * 批量删除key对应的键值
	 * @param key
	 * @return
	 */
	public static boolean delKeys(String type,String... keys) {
		boolean broken = false;
		try {
			if(null==jedis){
				 jedis = RedisHelper.getJedis();
				}
			if("clazz".equals(type)){
				jedis.select(ConfigUtils.ClazzDBIndex);
			}else
			if("list".equals(type)){
				jedis.select(ConfigUtils.ListDBIndex);
			}else
			if("table".equals(type)){
				jedis.select(ConfigUtils.TableDBIndex);
			}else{
				jedis.select(ConfigUtils.DBIndex);
			}
			jedis.del(keys);
			return true;
		}catch (JedisException ex) {
			broken = handleJedisException(ex);
			logger.error("批量删除key对应的键值，异常："+ ex);
			//returnBrokenResource(jedis);
		} finally {
			closeResource(jedis, broken);
		}
		return false;
	}
	
	
	/**
	 * 自增
	 * @param key
	 * @return
	 */
	public static long incr(String key) {
		boolean broken = false;
		try {
			if(null==jedis){
				 jedis = RedisHelper.getJedis();
				}
			jedis.select(ConfigUtils.DBIndex);
			return jedis.incr(key);
		}catch (JedisException ex) {
			broken = handleJedisException(ex);
			logger.error("自增incr error："+ ex);
		} finally {
			closeResource(jedis, broken);
		}
		return 0;
	}
	/**
	 * 自减
	 * @param key
	 * @return
	 */
	public static long decr(String key) {
		boolean broken = false;
		try {
			if(null==jedis){
				 jedis = RedisHelper.getJedis();
				}
			jedis.select(ConfigUtils.DBIndex);
			 return jedis.decr(key);
		} catch (JedisException ex) {
			broken = handleJedisException(ex);
			logger.error("自减incr error："+ ex);
		} finally {
			closeResource(jedis, broken);
		}
		return 0;
	}
	
	/*private static void returnBrokenResource(Jedis jedis) {
		try {
			if(null!=jedis)
			 RedisHelper.returnResource(jedis);
		} catch (Exception e) {
			logger.error("returnBrokenResource error.", e);
		}
	}
  
	private static void returnResource(Jedis jedis) {
		try {
			if(null!=jedis)
			  RedisHelper.returnResource(jedis);
		} catch (Exception e) {
			logger.error("returnResource error.", e);
		}
	}*/
	/**
	 * 字符串表达式，转换为计算表达式
	 * @param str
	 * @return
	 */
	public static Object Str2Express(String str){
        Object result=null;
		try {
			ScriptEngineManager manager = new ScriptEngineManager();  
			ScriptEngine engine = manager.getEngineByName("js"); 
			result = engine.eval(str);
			System.out.println("字符串表达式："+str+"|"+result);
		} catch (ScriptException e) {
			// TODO 自动生成的 catch 块
			e.printStackTrace();
		}  
        return result;
	}
	
	
	public static boolean handleJedisException(JedisException jedisException) {
	    if (jedisException instanceof JedisConnectionException) {
	        logger.error("Redis connection 异常：", jedisException);
	    } else if (jedisException instanceof JedisDataException) {
	        if ((jedisException.getMessage() != null) && (jedisException.getMessage().indexOf("READONLY") != -1)) {
	            logger.error("Redis connection  are read-only slave.", jedisException);
	        } else {
	            // dataException, isBroken=false
	            return false;
	        }
	    } else {
	        logger.error("Jedis exception happen.", jedisException);
	    }
	    return true;
	}
	/**
	 * Return jedis connection to the pool, call different return methods depends on the conectionBroken status.
	 */
	public static void closeResource(Jedis jedis, boolean conectionBroken) {
	    try {
	        if (conectionBroken) {
	        	RedisHelper.returnBrokenResource(jedis);
	        } else {
	        	RedisHelper.returnResource(jedis);
	        }
	    } catch (Exception e) {
	    	destroyJedis(jedis);
	    }
	}
	
	public static void destroyJedis(Jedis jedis) {
        if ((jedis != null) && jedis.isConnected()) {
            try {
                try {
                    jedis.quit();
                }
                catch (Exception e) {
                }
                jedis.disconnect();
            }
            catch (Exception e) {
            	logger.error("return back jedis failed, will fore close the jedis.", e);
            }
        }
    }
	
	
}