package com.easyframework.redis.api;

import java.util.List;
import java.util.Map;


public class SyncJedisHelper {

	private static final SyncJedisHelper jedisDB = new SyncJedisHelper();
	private static final Jedisdao jedisdao=new JedisConvertCore();
	
	public static SyncJedisHelper getInstance() {
		return jedisDB;
	}
	
	public String ClearDBData(int indexDB){
		return jedisdao.ClearDBData(indexDB);
	}
	
	public boolean         save(Object obj){
		return jedisdao.save(obj);
	}
	public boolean         save(Object obj,int indexDB){
		return jedisdao.save(obj, indexDB);
	}
	public boolean 		   save(Object obj,String userphone,int indexdb){
		return jedisdao.save(obj, userphone, indexdb);
	}
	
	public Object           get(Class<?> claszz,String id){
		return jedisdao.get(claszz, id);
	}
	public Object           get(Class<?> claszz,String id,int indexDB){
		return jedisdao.get(claszz, id, indexDB);
	}
	
	public  List<?>   query(Class<?> claszz){
		return jedisdao.query(claszz);
	}
	public  List<?>   query(Class<?> claszz,int indexDB){
		return jedisdao.query(claszz, indexDB);
	}
	
	public  List<?>   queryByWno(Class<?> claszz,String where){
		return jedisdao.queryByWno(claszz, where);
	}
	public  List<?>   queryByWno(Class<?> claszz,String where,int indexDB){
		return jedisdao.queryByWno(claszz, where, indexDB);
	}
	public  List<?>   queryByLess(Class<?> claszz,String where){
		return jedisdao.queryByLess(claszz, where);
	}
	public  List<?>   queryByLess(Class<?> claszz,String where,int indexDB){
		return jedisdao.queryByLess(claszz, where, indexDB);
	}
	
	public  List<?>   queryByNoLess(Class<?> claszz,String no,String where){
		return jedisdao.queryByNoLess(claszz, no, where);
	}
	public  List<?>   queryByNoLess(Class<?> claszz,String no,String where,int indexDB){
		return jedisdao.queryByNoLess(claszz, no, where, indexDB);
	}
	
	
	
	//---------------------读redis数据没有从mysql操作R读数据-------------------------------------	
	//字符串信息存取
	public  boolean         setMsg(String key,String value){
		return jedisdao.setMsg(key, value);
	}
	public  String          getMsg(String key){
		return jedisdao.getMsg(key);
	}
	
	//通过查询sql作为key键标识，查出保存的所有对象
	
	//对象(Object、数组)转换为字节数组存取
	//备注：模型对象必须实现序列化
	public  boolean         setObject(Class<?> clazz,String key,Object obj){
		return jedisdao.setObject(clazz, key, obj);
	}
	public  Object          getObject(Class<?> clazz,String key){
		return jedisdao.getObject(clazz, key);
	}
	
	//(list、List<obj>)转换为字节数组存取
	//备注：模型对象必须实现序列化
	public  boolean                setListBySQL(Class<?> clazz,String sqlkey,Object obj){
		return jedisdao.setListBySQL(clazz, sqlkey, obj);
	}
	public  List<?>              queryListBySQL(Class<?> clazz,String sqlkey){
		return jedisdao.queryListBySQL(clazz, sqlkey);
	}
/*	public  boolean 		    syncUpdateListBySQL(Class<?> clazz,String sqlkey,Object obj);
	public  boolean 		    syncDeleteListBySQL(Class<?> clazz,String sqlkey);*/
	
	//将list转换为字符串格式存储
	//备注：模型对象不需要实现序列化
	public  boolean             setStrListBySQL(String sqlkey,List<?> list){
		return jedisdao.setStrListBySQL(sqlkey, list);
	}
	public  <T>T              queryStrListBySQL(Class<T> clazz,String sqlkey){
		return jedisdao.queryStrListBySQL(clazz, sqlkey);
	}
	
	//提前准备的同步操作
	public  boolean 		 syncUpdateStrListBySQL(Class<?> clazz,String sqlkey,List<?> list){
		return jedisdao.syncUpdateStrListBySQL(clazz, sqlkey, list);
	}
	public  boolean 		 syncDeleteStrListBySQL(Class<?> clazz,String sqlkey){
		return jedisdao.syncDeleteStrListBySQL(clazz, sqlkey);
	}
	
	//将map转换为字符串
	public  boolean              setStrMapBySQL(String sqlkey,Map<String,String> map){
		return jedisdao.setStrMapBySQL(sqlkey, map);
	}
	public  Map<String,String> queryStrMapBySQL(String sqlkey){
		return jedisdao.queryStrMapBySQL(sqlkey);
	}
	
	public  boolean         		setMapBySQL(String sqlkey,Map<String,?> map){
		return jedisdao.setMapBySQL(sqlkey, map);
	}
	public  Map<String,?> 		  queryMapBySQL(String sqlkey){
		return jedisdao.queryMapBySQL(sqlkey);
	}
	
	//-------------------从mysql操作CUD后操作redis数据同步----------------------------------------------
	//提前准备的同步操作
	public  boolean 		    syncSaveTable2Redis(Object obj,String type){
		return jedisdao.syncSaveTable2Redis(obj, type);
	}
	
	public  boolean syncDeleteTable2Redis(Object obj){
		return jedisdao.syncDeleteTable2Redis(obj);
	}
	
	
	public  boolean 		  syncUpdateTable2Redis(Object obj,String type){
		return jedisdao.syncUpdateTable2Redis(obj, type);
	}
	
	public  boolean 		  syncDeleteTable2Redis(Object obj,String type){
		return jedisdao.syncDeleteTable2Redis(obj, type);
	}
	
	public  boolean 		  syncDeleteTable2Redis(Class<?> clazz,String type,String id){
		return jedisdao.syncDeleteTable2Redis(clazz, type, id);
	}
	public  boolean 		  syncDeleteTable2Redis(Class<?> clazz,String type,String... id){
		return jedisdao.syncDeleteTable2Redis(clazz, type, id);
	}
	
	
	public  boolean 		  syncDeleteTable2Redis(Class<?> clazz,String type,Integer id){
		return jedisdao.syncDeleteTable2Redis(clazz, type, id);
	}
	
	public  boolean 		  syncDeleteTable2Redis(Class<?> clazz,String type,Integer... id){
		return jedisdao.syncDeleteTable2Redis(clazz, type, id);
	}
	
	
	public  boolean 				setListMapBySQL(String sqlkey, List<Map<String, Object>> list){
		return jedisdao.setListMapBySQL(sqlkey, list);
	}
	public	List<Map<String, Object>> 	queryListMapBySQL(String sqlkey){
		return jedisdao.queryListMapBySQL(sqlkey);
	}
	
}
