package com.easyframework.redis.api;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.Serializable;

import org.apache.log4j.Logger;

public class ObjectsTranscoder<M extends Serializable> extends SerializeTranscoder {

	private static Logger logger = Logger.getLogger(ObjectsTranscoder.class);
	  
	  @SuppressWarnings("unchecked")
	  @Override
	  public byte[] serialize(Object value) {
	    if (value == null) {  
	      throw new NullPointerException("Can't serialize null");  
	    }  
	    byte[] result = null;  
	    ByteArrayOutputStream bos = null;  
	    ObjectOutputStream os = null;  
	    try {  
	      bos = new ByteArrayOutputStream();  
	      os = new ObjectOutputStream(bos);
	      M m = (M) value;
	      os.writeObject(m);  
	      os.close();  
	      bos.close();  
	      result = bos.toByteArray();  
	    } catch (IOException e) {  
	      throw new IllegalArgumentException("Non-serializable object", e);  
	    } finally {  
	      close(os);  
	      close(bos);  
	    }  
	    return result;  
	  }

	  @SuppressWarnings("unchecked")
	  @Override
	  public M deserialize(byte[] in) {
	    M result = null;  
	    ByteArrayInputStream bis = null;  
	    ObjectInputStream is = null;  
	    try {  
	      if (in != null) {  
	        bis = new ByteArrayInputStream(in);  
	        is = new ObjectInputStream(bis);  
	        result = (M) is.readObject();  
	        is.close();  
	        bis.close();  
	      }  
	    } catch (IOException e) {  
	    	logger.error("Caught IOException decoding bytes of data",e);  
	    } catch (ClassNotFoundException e) {  
	    	logger.error("Caught CNFE decoding %d bytes of data",e);  
	    } finally {  
	      close(is);  
	      close(bis);  
	    }  
	    return result;  
	  }
	}