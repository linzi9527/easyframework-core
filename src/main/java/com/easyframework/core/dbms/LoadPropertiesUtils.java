package com.easyframework.core.dbms;

import java.util.ResourceBundle;

import org.apache.log4j.Logger;

import com.easyframework.core.db.StringUtil;

public class LoadPropertiesUtils {
	private static Logger logger = Logger.getLogger(LoadPropertiesUtils.class);
	//private static ResourceBundle  BUNDLE       = ResourceBundle.getBundle("db-ms");
	public static  ResourceBundle BUNDLE =null;
	public static boolean Develop_Mode=false;
	public static boolean SQL_FORMAT=false;

	public static boolean Is_MS_OPEN=false;//是否开启主从配置
	public static boolean db_Master_W=false;
	public static int ms=1;
	
	public static boolean db_Slave_R  =false;
	public static boolean db_Slave02_R=false;
	public static boolean db_Slave03_R=false;
	public static boolean db_Slave04_R=false;
	public static boolean db_Slave05_R=false;
	
	static {
		try {
			BUNDLE = ResourceBundle.getBundle("db-ms");
			} catch (Exception e) {
				logger.error("主从db-ms配置文件加载异常:"+e.getMessage());
			}
		try {
			if(null!=BUNDLE){
				Develop_Mode=StringUtil.StringToBoolean(BUNDLE.getString("develop_mode"));
			      SQL_FORMAT=StringUtil.StringToBoolean(BUNDLE.getString("sql_format"));
			}
		} catch (Exception e) {
			logger.error("配置文件加载Develop_Mode,SQL_FORMAT异常:"+e.getMessage());
		}
		try {
			if(null!=BUNDLE){
			Is_MS_OPEN =StringUtil.StringToBoolean(BUNDLE.getString("Is_MS_OPEN"));
			db_Master_W=StringUtil.StringToBoolean(BUNDLE.getString("db_Master_W"));
			}
		} catch (Exception e) {
			logger.error("获取Is_MS_OPEN,db_Master_W异常："+e.getMessage());
		}
		try {
			if(null!=BUNDLE){
			 ms=StringUtil.StringToInteger(BUNDLE.getString("master_slave_type"));
			}
		} catch (Exception e1) {
			logger.error("获取MS_TYPE异常:"+e1.getMessage());
		}
		////////////////////////////////////////////////////////////////////////////////////////
		if(null!=BUNDLE&&Is_MS_OPEN&&db_Master_W){
		 try {
			 db_Slave_R=StringUtil.StringToBoolean(BUNDLE.getString("db_Slave_R"));
			} catch (Exception e1) {
				logger.info("警告：db_Slave_R读取");
			} 
		 try {
			 db_Slave02_R=StringUtil.StringToBoolean(BUNDLE.getString("db_Slave02_R"));
			} catch (Exception e1) {
				logger.info("警告：db_Slave02_R读取");
			} 
		 
		 try {
			 db_Slave03_R=StringUtil.StringToBoolean(BUNDLE.getString("db_Slave03_R"));
			} catch (Exception e1) {
				logger.info("警告：db_Slave03_R读取");
			} 
		 try {
			 db_Slave04_R=StringUtil.StringToBoolean(BUNDLE.getString("db_Slave04_R"));
			} catch (Exception e1) {
				logger.info("警告：db_Slave04_R读取");
			} 
		 try {
			 db_Slave05_R=StringUtil.StringToBoolean(BUNDLE.getString("db_Slave05_R"));
			} catch (Exception e1) {
				logger.info("警告：db_Slave05_R读取");
			} 
		} 
	}
}
