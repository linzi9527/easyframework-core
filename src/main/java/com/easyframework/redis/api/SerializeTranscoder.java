package com.easyframework.redis.api;

import java.io.Closeable;

import org.apache.log4j.Logger;
/**
 * 通过研究Jedis的API，我们发现其提供了这样的API

jedis.set(byte[], byte[]),

通过这个API，很显然我们能够实现

jedis.set(String key, Object value)

jedis.set(String key, List<M> values)

我们需要关注的就是在cache的时候将Object和List对象转换成字节数组并且需要提供将字节数组转换成对象返回

考虑到扩展性，设计了3个类，抽象父类SerializeTranscoder， 子类ListTranscoder和ObjectsTranscoder
以及一个Unit test 类 用于模拟对象，list对象和字节数组的转换，即Serialize和de-searilize的过程
 * @author 蓝眼泪
 * 2018-2-5下午5:27:33
 */
public abstract class SerializeTranscoder {

	  protected static Logger logger = Logger.getLogger(SerializeTranscoder.class);
	  
	  public abstract byte[] serialize(Object value);
	  
	  public abstract Object deserialize(byte[] in);
	  
	  public void close(Closeable closeable) {
	    if (closeable != null) {
	      try {
	        closeable.close();
	      } catch (Exception e) {
	         logger.info("Unable to close " + closeable, e); 
	      }
	    }
	  }
	}