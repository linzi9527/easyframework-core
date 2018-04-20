package com.easyframework.redis.conf;

import java.io.IOException;
import java.io.InputStream;
import java.util.Properties;

import org.apache.log4j.Logger;


public class ConfigUtils {
	private static Logger logger = Logger.getLogger(ConfigUtils.class);
	private static Properties prop = new Properties();	
	
	
	public static  String RedisIP     ="127.0.0.1";
	public static  int RedisPort      =6379;
	
	
	public static  int DBIndex        =0,
					   ListDBIndex    =0,
					   TableDBIndex   =0,
					   ClazzDBIndex   =0;
	
	public static  int defaultExpire  =60,
						  listExpire  =60,
						 tableExpire  =60,
						 clazzExpire  =60;
						  
	public static  boolean Lifo       =false;		
	public static  int MaxTotal       =10;
	public static  long MaxWaitMillis =-1;
	
	public static  int Maxidle        =100,Minidle=10;
	
	public static  boolean Borrow     =false,WhileIdle  =false;
	public static  long MinEvictableIdleTimeMillis     =1800;
	
	public static  boolean isDebug =false;
	
	static{		
		try {
				logger.info("\n加载redis.properties配置文件...");
				InputStream in = ConfigUtils.class.getResourceAsStream("/redis.properties");
				prop.load(in);
				in.close();
				logger.info("\n配置文件加载完成!");
		} catch (IOException e) {
			// TODO 自动生成的 catch 块
			logger.info("\n加载redis.properties配置文件异常：",e);
			//e1.printStackTrace();
		}
		
		
		//-------------redis--------------------------
		 try {
			RedisIP        =prop.getProperty("redis_ip");
			RedisPort      =Integer.parseInt(prop.getProperty("redis_port"));
		} catch (Exception e) {
			// TODO Auto-generated catch block
			//e.printStackTrace();
			logger.info("\n配置RedisIP:"+RedisIP+
					",\nRedisPort:"+RedisPort);
		}
		 
		try {
			DBIndex        =Integer.parseInt(prop.getProperty("default_db_index"));
			ListDBIndex    =Integer.parseInt(prop.getProperty("list_db_index"));
			TableDBIndex   =Integer.parseInt(prop.getProperty("table_db_index"));
			ClazzDBIndex   =Integer.parseInt(prop.getProperty("clazz_db_index"));
		} catch (Exception e) {
			// TODO Auto-generated catch block
			logger.info("\n配置DBIndex:"+DBIndex+
					",\nListDBIndex:"+ListDBIndex+
					",\nTableDBIndex:"+TableDBIndex+
					",\nClazzDBIndex:"+ClazzDBIndex);
		} 
		 
		 
		try {
			defaultExpire  =Integer.parseInt(prop.getProperty("default_expire"));
			listExpire     =Integer.parseInt(prop.getProperty("list_expire"));
			tableExpire    =Integer.parseInt(prop.getProperty("table_expire"));
			clazzExpire    =Integer.parseInt(prop.getProperty("clazz_expire"));
		} catch (Exception e) {
			// TODO Auto-generated catch block
			logger.info("\n配置defaultExpire:"+defaultExpire+
					",\nlistExpire:"+listExpire+
					",\ntableExpire:"+tableExpire+
					",\nclazzExpire:"+clazzExpire);
		} 
		
		//-----------------------------------------
		
		try {
			Lifo           =Boolean.parseBoolean(prop.getProperty("lifo"));
			MaxTotal       =Integer.parseInt(prop.getProperty("maxTotal"));
			MaxWaitMillis  =Long.parseLong(prop.getProperty("maxWaitMillis"));
			Maxidle        =Integer.parseInt(prop.getProperty("maxidle"));
			Minidle        =Integer.parseInt(prop.getProperty("minidle"));
			Borrow         =Boolean.parseBoolean(prop.getProperty("testOnBorrow"));
			WhileIdle      =Boolean.parseBoolean(prop.getProperty("testWhileIdle"));
			MinEvictableIdleTimeMillis     =Long.parseLong(prop.getProperty("minEvictableIdleTimeMillis"));
		} catch (Exception e) {
			// TODO Auto-generated catch block
			logger.info("\n配置Lifo:"+Lifo+
					",\nMaxTotal:"+MaxTotal+
					",\nMaxWaitMillis:"+MaxWaitMillis+
					",\nMaxidle:"+Maxidle+
					",\nMinidle:"+Minidle+
					
					",\nBorrow:"+Borrow+
					",\nWhileIdle:"+WhileIdle+
					",\nMinEvictableIdleTimeMillis:"+MinEvictableIdleTimeMillis
					);
		} 
		
		try {
			isDebug    =Boolean.parseBoolean(prop.getProperty("debug"));
		} catch (Exception e) {
			logger.info("\n配置isDebug:"+isDebug);
		}
		 
	}
	
	

}