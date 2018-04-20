package com.easyframework.redis.api;

import java.util.List;
import java.util.Map;

public interface Jedisdao {
	public String ClearDBData( int indexDB);
	
	public boolean         save(Object obj);
	public boolean         save(Object obj,int indexDB);
	public boolean 		   save(Object obj,String userphone,int indexdb);
	
	public Object           get(Class<?> claszz,String id);
	public Object           get(Class<?> claszz,String id,int indexDB);
	
	public  List<?>   query(Class<?> claszz);
	public  List<?>   query(Class<?> claszz,int indexDB);
	
	public  List<?>   queryByWno(Class<?> claszz,String where);
	public  List<?>   queryByWno(Class<?> claszz,String where,int indexDB);
	public  List<?>   queryByLess(Class<?> claszz,String where);
	public  List<?>   queryByLess(Class<?> claszz,String where,int indexDB);
	
	public  List<?>   queryByNoLess(Class<?> claszz,String no,String where);
	public  List<?>   queryByNoLess(Class<?> claszz,String no,String where,int indexDB);
	
	
	
	//---------------------读redis数据没有从mysql操作R读数据-------------------------------------	
	//字符串信息存取
	public  boolean         setMsg(String key,String value);
	public  String          getMsg(String key);
	
	//通过查询sql作为key键标识，查出保存的所有对象
	
	//对象(Object、数组)转换为字节数组存取
	//备注：模型对象必须实现序列化
	public  boolean         setObject(Class<?> clazz,String key,Object obj);
	public  Object          getObject(Class<?> clazz,String key);
	
	//(list、List<obj>)转换为字节数组存取
	//备注：模型对象必须实现序列化
	public  boolean                setListBySQL(Class<?> clazz,String sqlkey,Object obj);
	public  List<?>              queryListBySQL(Class<?> clazz,String sqlkey);
/*	public  boolean 		    syncUpdateListBySQL(Class<?> clazz,String sqlkey,Object obj);
	public  boolean 		    syncDeleteListBySQL(Class<?> clazz,String sqlkey);*/
	
	//将list转换为字符串格式存储
	//备注：模型对象不需要实现序列化
	public  boolean             setStrListBySQL(String sqlkey,List<?> list);
	public  <T>T              queryStrListBySQL(Class<T> clazz,String sqlkey);
	
	//提前准备的同步操作
	public  boolean 		 syncUpdateStrListBySQL(Class<?> clazz,String sqlkey,List<?> list);
	public  boolean 		 syncDeleteStrListBySQL(Class<?> clazz,String sqlkey);
	
	//将map转换为字符串
	public  boolean              setStrMapBySQL(String sqlkey,Map<String,String> map);
	public  Map<String,String> queryStrMapBySQL(String sqlkey);
	
	public  boolean         		setMapBySQL(String sqlkey,Map<String,?> map);
	public  Map<String,?> 		  queryMapBySQL(String sqlkey);
	
	//-------------------从mysql操作CUD后操作redis数据同步----------------------------------------------
	//提前准备的同步操作
	public  boolean 		    syncSaveTable2Redis(Object obj,String type);
	public  boolean 		  syncUpdateTable2Redis(Object obj,String type);
	
	
	public  boolean 		  syncDeleteTable2Redis(Object obj);
	public  boolean 		  syncDeleteTable2Redis(Object obj,String type);
	public  boolean 		  syncDeleteTable2Redis(Class<?> clazz,String type,String id);
	public  boolean 		  syncDeleteTable2Redis(Class<?> clazz,String type,Integer id);
	
	public  boolean 		  syncDeleteTable2Redis(Class<?> clazz,String type,String... id);
	public  boolean 		  syncDeleteTable2Redis(Class<?> clazz,String type,Integer... id);


	public  boolean 				setListMapBySQL(String sqlkey, List<Map<String, Object>> list);
	public	List<Map<String, Object>> 	queryListMapBySQL(String sqlkey);
	
}
