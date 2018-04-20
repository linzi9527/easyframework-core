package com.easyframework.core.db;

import java.util.ResourceBundle;

import org.apache.log4j.Logger;

public class PropertiesUtils {
	private static Logger logger = Logger.getLogger(PropertiesUtils.class);
	//private static ResourceBundle  BUNDLE       = ResourceBundle.getBundle("db-ms");
	public static  ResourceBundle BUNDLE_db =null;
	public static  ResourceBundle BUNDLE_dbms =null;
	public static boolean Develop_Mode=false;
	public static boolean SQL_FORMAT=false;
	public static boolean JSON_LOG=false;

	public static boolean Is_MS_OPEN=false;//是否开启主从配置
	public static boolean db_Master_W=false;
	
	public static boolean Is_Redis_Open=false;//是否开启redis数据同步
	//redis主从即1表示只有一个从库redis;2表示两个从库;3表示三个从库
	//public static int redis_master_slave_type=3;
	//redis_master_slave_type只有当Is_Redis_Open为true时有效
	static {
		/////////////////////////单库参数配置///////////////////////////////////////
		try {
			BUNDLE_db = ResourceBundle.getBundle("db");
			} catch (Exception e) {
				logger.error("\n主从db.properties配置文件加载:"+e.getMessage());
			}
			try {
				JSON_LOG=StringUtil.StringToBoolean(BUNDLE_db.getString("json_log"));
			} catch (Exception e) {
				logger.info("\n配置参数JSON_LOG:"+JSON_LOG);
			}
		if(null!=BUNDLE_db){
			try {
				      SQL_FORMAT=StringUtil.StringToBoolean(BUNDLE_db.getString("sql_format"));
				      Develop_Mode=StringUtil.StringToBoolean(BUNDLE_db.getString("develop_mode"));
			} catch (Exception e) {
				logger.info("\n配置参数Develop_Mode:"+Develop_Mode+",SQL_FORMAT:"+SQL_FORMAT);
			}
			//----------加载是否开启redis数据同步--只开启单机一个redis-------------
			try {
				Is_Redis_Open=StringUtil.StringToBoolean(BUNDLE_db.getString("Is_Redis_Open"));
			} catch (Exception e) {
				logger.info("\n配置参数是否开启redis数据同步Is_Redis_Open:"+Is_Redis_Open);
			}

		}else{
		
		////////////////////////////主从配置参数////////////////////////////////////////////////////////////
		try {
			BUNDLE_dbms = ResourceBundle.getBundle("db-ms");
			} catch (Exception e) {
				logger.error("\n主从db-ms.properties配置文件加载异常:"+e.getMessage());
			}

		if(null!=BUNDLE_dbms){
			try {
				Is_MS_OPEN =StringUtil.StringToBoolean(BUNDLE_dbms.getString("Is_MS_OPEN"));
				db_Master_W=StringUtil.StringToBoolean(BUNDLE_dbms.getString("db_Master_W"));
				
			} catch (Exception e) {
				logger.info("\n配置参数Is_MS_OPEN:"+Is_MS_OPEN+",db_Master_W："+db_Master_W);
			}
			try {
					Develop_Mode=StringUtil.StringToBoolean(BUNDLE_dbms.getString("develop_mode"));
				      SQL_FORMAT=StringUtil.StringToBoolean(BUNDLE_dbms.getString("sql_format"));
			} catch (Exception e) {
				logger.info("\n配置参数Develop_Mode:"+Develop_Mode+",SQL_FORMAT:"+SQL_FORMAT);
			}
			//----------加载是否开启redis数据同步--开启主从redis-------------
			try {
				Is_Redis_Open=StringUtil.StringToBoolean(BUNDLE_db.getString("Is_Redis_Open"));
			} catch (Exception e) {
				logger.info("\n配置参数是否开启redis数据同步Is_Redis_Open:"+Is_Redis_Open);
			}
		 }
		}
	}
}
