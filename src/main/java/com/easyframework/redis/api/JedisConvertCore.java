package com.easyframework.redis.api;

import java.lang.reflect.Field;
import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;

import org.apache.commons.beanutils.BeanUtils;

import com.easyframework.core.a.Colum;
import com.easyframework.core.a.Key;
import com.easyframework.core.a.Table;
import com.easyframework.redis.core.JedisUtil;
import com.easyframework.redis.core.List2Json;



public class JedisConvertCore implements Jedisdao{

	public  byte[] strToByteArray(String str) {
	    if (str == null) {
	        return null;
	    }
	    byte[] byteArray = str.getBytes();
	    return byteArray;
	}
	/**
	 * 获得对象属性的值
	 */
	public static Object invokeMethod(Object owner, String methodName,Object[] args) throws Exception {
		Class<?> ownerClass = owner.getClass();
		methodName = methodName.substring(0, 1).toUpperCase()+ methodName.substring(1);
		Method method = null;
		try {
			method = ownerClass.getMethod("get" + methodName);
		} catch (SecurityException e) {
		} catch (NoSuchMethodException e) {
			return " can't find 'get" + methodName + "' method";
		}
		return method.invoke(owner);
	}
	
	public  boolean save(Object obj) {
		String tableName=null;
		String rowkey=null;
		boolean result=false;
	
		boolean isExistTableName = obj.getClass().isAnnotationPresent(Table.class);
		if (isExistTableName) {
			Table pojo = (Table)obj.getClass().getAnnotation(Table.class);
		    if(pojo!=null) tableName=pojo.name();
		     Class<?> clazz=  obj.getClass();
		     Field field[] =   clazz.getDeclaredFields();
		     int count=0;
		 	for (int i=0;i<field.length;i++) {
				Field f =field[i];
				try {
					 Key k= f.getAnnotation(Key.class);
					if(k!=null){
						Object value = invokeMethod(obj, f.getName(), null);
						 if(value!=null) 
							 rowkey=value.toString();
						 else 
							 rowkey=UUID.randomUUID().toString();
						 	
						 	String key=tableName+":"+rowkey+":"+f.getName();
						 	boolean ff=JedisUtil._set(key, rowkey,"clazz");
						 	if(ff) count++;
					}else{
						//按注解字段存
						//Colum col = f.getAnnotation(Colum.class);
						//String columName=col.columName();
						//String key=tableName+":"+rowkey+":"+columName;
						//按vo属性名存
						String key=tableName+":"+rowkey+":"+f.getName();
							Object value = invokeMethod(obj, f.getName(), null);
							String val="";
							if(value!=null){val=value.toString();}
							boolean flag=JedisUtil._set(key, val,"clazz");
							if(flag) count++;
					}
				} catch (Exception e) {
					// TODO 自动生成的 catch 块
					e.printStackTrace();
				}
				
		 	}//for
		 	//System.out.println("count:"+count+"|field.length:"+field.length);
		 	result=	count==field.length ? true:false;
		}
		
		
		return result;
	}
	
	
	/**
	 * 选择数据要保存的数据库下标
	 */
	public  boolean save(Object obj,int indexdb) {
		String tableName=null;
		String rowkey=null;
		boolean result=false;
	
		boolean isExistTableName = obj.getClass().isAnnotationPresent(Table.class);
		if (isExistTableName) {
			Table pojo = (Table)obj.getClass().getAnnotation(Table.class);
		    if(pojo!=null) tableName=pojo.name();
		     Class<?> clazz=  obj.getClass();
		     Field field[] =   clazz.getDeclaredFields();
		     int count=0;
		 	for (int i=0;i<field.length;i++) {
				Field f =field[i];
				try {
					 Key k= f.getAnnotation(Key.class);
					if(k!=null){
						Object value = invokeMethod(obj, f.getName(), null);
						 if(value!=null) 
							 rowkey=value.toString();
						 else 
							 rowkey=UUID.randomUUID().toString();
						 	
						 	String key=tableName+":"+rowkey+":"+f.getName();
						 	boolean ff=JedisUtil.set(key, rowkey,indexdb);

						 	 if(ff) count++;
					}else{
						//按注解字段存
						//Colum col = f.getAnnotation(Colum.class);
						//String columName=col.columName();
						//String key=tableName+":"+rowkey+":"+columName;
						String key=tableName+":"+rowkey+":"+f.getName();
							Object value = invokeMethod(obj, f.getName(), null);
							String val="0";
							if(value==null){}else{val=value.toString();}
							boolean flag=JedisUtil.set(key, val,indexdb);
							if(flag) count++;
					}
				} catch (Exception e) {
					// TODO 自动生成的 catch 块
					e.printStackTrace();
				}
				
		 	}//for
		 	//System.out.println("count:"+count+"|field.length:"+field.length);
		 	result=	count==field.length ? true:false;
		}
		
		
		return result;
	}
	

	
	@Override
	public Object get(Class<?> claszz,String id) {
		// TODO Auto-generated method stub
		String tableName=null;
		 Object obj=null;
		try {
			 obj =  claszz.newInstance();
			boolean isExistTableName = obj.getClass().isAnnotationPresent(Table.class);
			if (isExistTableName) {
				Table pojo = (Table)obj.getClass().getAnnotation(Table.class);
			    if(pojo!=null) tableName=pojo.name();
			       Class<?> instance = obj.getClass();
			     Field field[] =  instance.getDeclaredFields();
			     int count=0; 		  
			 	for (int i=0;i<field.length;i++) {
					Field f =field[i];
					try {
						 Key k= f.getAnnotation(Key.class);
						 String columName=f.getName();
						
						if(k!=null&&k.isPrimary()){
							BeanUtils.copyProperty(obj,columName,id);
						}else{
							String val=JedisUtil._get(tableName+":"+id+":"+columName,"clazz");
							if(val!=null){
								BeanUtils.copyProperty(obj,columName,val);
							}else{count++;}
						}
					} catch (Exception e) {
						// TODO 自动生成的 catch 块
						e.printStackTrace();
					}
					
				//System.out.println(pojo.getClass());
			 	}//end for
			     if(count==(field.length-1)){return null;}
			}
		} catch (InstantiationException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (IllegalAccessException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		return obj;
	}
	@Override
	public Object get(Class<?> claszz,String id,int indexdb) {
		// TODO Auto-generated method stub
		String tableName=null;
		 Object obj=null;
		try {
			 obj =  claszz.newInstance();
			boolean isExistTableName = obj.getClass().isAnnotationPresent(Table.class);
			if (isExistTableName) {
				Table pojo = (Table)obj.getClass().getAnnotation(Table.class);
			    if(pojo!=null) tableName=pojo.name();
			       Class<?> instance = obj.getClass();
			     Field field[] =  instance.getDeclaredFields();
			      		  
			 	for (int i=0;i<field.length;i++) {
					Field f =field[i];
					try {
						 Key k= f.getAnnotation(Key.class);
						 String columName=f.getName();
						
						if(k!=null&&k.isPrimary()){
							BeanUtils.copyProperty(obj,columName,id);
						}else{
							String val=JedisUtil.get(tableName+":"+id+":"+columName,indexdb);
							BeanUtils.copyProperty(obj,columName,val);
						}
					} catch (Exception e) {
						// TODO 自动生成的 catch 块
						e.printStackTrace();
					}
					
//					System.out.println(pojo.getClass());
			 	}
			     
			}
		} catch (InstantiationException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (IllegalAccessException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		return obj;
	}
	@Override
	public  List<?> query(Class<?> claszz,int indexdb) {
		// TODO Auto-generated method stub
		String tableName=null;
		 List<Object> list=new ArrayList<Object>();
		try {
			boolean isExistTableName = claszz.newInstance().getClass().isAnnotationPresent(Table.class);
			if (isExistTableName) {
				Table pojo = (Table) claszz.newInstance().getClass().getAnnotation(Table.class);
			    if(pojo!=null) tableName=pojo.name();
			      		   
			    Set<String>	sset= JedisUtil.getKeys(tableName+"*",indexdb);
			    if(sset!=null&&sset.size()>0){  	
			    	Set<String> obj_keys=JedisUtil.getIds(sset);
			    	for(String id:obj_keys){
					 	list.add(this.get(claszz,id,indexdb));
					  }
			    	//System.out.println("list-size:"+list.size());
			    } 
			}
		} catch (InstantiationException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (IllegalAccessException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	
		return list;
	}

	
	@Override
	public  List<?> query(Class<?> claszz) {
		// TODO Auto-generated method stub
		String tableName=null;
		 List<Object> list=new ArrayList<Object>();
		try {
			boolean isExistTableName = claszz.newInstance().getClass().isAnnotationPresent(Table.class);
			if (isExistTableName) {
				Table pojo = (Table) claszz.newInstance().getClass().getAnnotation(Table.class);
			    if(pojo!=null) tableName=pojo.name();
			      		   
			    Set<String>	sset= JedisUtil._getKeys(tableName+"*","clazz");
			    if(sset!=null&&sset.size()>0){  	
			    	Set<String> obj_keys=JedisUtil.getIds(sset);
			    	for(String id:obj_keys){
					 	list.add(this.get(claszz,id));//id是（xy0001:id）
					  }
			    	//System.out.println("list-size:"+list.size());
			    } 
			}
		} catch (InstantiationException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (IllegalAccessException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	
		return list;
	}
	
	
	@Override
	public List<?> queryByWno(Class<?> claszz, String wno,int indexdb) {
		// TODO 自动生成的方法存根
		String tableName=null;
		 List<Object> list=new ArrayList<Object>();
		try {
			boolean isExistTableName = claszz.newInstance().getClass().isAnnotationPresent(Table.class);
			if (isExistTableName) {
				Table pojo = (Table) claszz.newInstance().getClass().getAnnotation(Table.class);
			    if(pojo!=null) tableName=pojo.name();
			    Set<String>	sset= JedisUtil.getKeys(tableName+":"+wno+"*",indexdb);
			    if(sset!=null&&sset.size()>0){  	
			    	Set<String> obj_keys=JedisUtil.getIds(sset);
			    	for(String id:obj_keys){
			    		if(id.contains(wno)&&id.startsWith(wno))
					 	list.add(this.get(claszz,id,indexdb));
					  }
			    	//System.out.println("list-size:"+list.size());
			    } 
			}
		} catch (InstantiationException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (IllegalAccessException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		return list;
	}
	
	
	
	
	@Override
	public List<?> queryByWno(Class<?> claszz, String wno) {
		// TODO 自动生成的方法存根
		String tableName=null;
		 List<Object> list=new ArrayList<Object>();
		try {
			boolean isExistTableName = claszz.newInstance().getClass().isAnnotationPresent(Table.class);
			if (isExistTableName) {
				Table pojo = (Table) claszz.newInstance().getClass().getAnnotation(Table.class);
			    if(pojo!=null) tableName=pojo.name();
			    Set<String>	sset= JedisUtil.getKeys(tableName+":"+wno+"*");
			    if(sset!=null&&sset.size()>0){  	
			    	Set<String> obj_keys=JedisUtil.getIds(sset);
			    	for(String id:obj_keys){
			    		if(id.contains(wno)&&id.startsWith(wno))
					 	list.add(this.get(claszz,id));
					  }
			    	//System.out.println("list-size:"+list.size());
			    } 
			}
		} catch (InstantiationException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (IllegalAccessException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		return list;
	}
	/**
	 * 小于：e.g:
	 * where :<60
	 */
	@Override
	public List<?> queryByLess(Class<?> claszz, String where) {
		// TODO 自动生成的方法存根
		String tableName=null;
		 List<Object> list=new ArrayList<Object>();
		 long now=new Date().getTime();
		try {
			boolean isExistTableName = claszz.newInstance().getClass().isAnnotationPresent(Table.class);
			if (isExistTableName) {
				Table pojo = (Table) claszz.newInstance().getClass().getAnnotation(Table.class);
			    if(pojo!=null) tableName=pojo.name();
			    Set<String>	sset= JedisUtil.getKeys(tableName+"*");
			    if(sset!=null&&sset.size()>0){  	
			    	Set<String> obj_keys=JedisUtil.getIds(sset);
			    	for(String noid:obj_keys){
			    		String id=noid.substring(noid.indexOf(":")+1, noid.length());
			    		long t=(now -Long.parseLong(id))/1000;
			    		boolean key=(boolean)JedisUtil.Str2Express(t+where);
					 	if(key)list.add(this.get(claszz,noid));
					  }
			    	//System.out.println("list-size:"+list.size());
			    } 
			}
		} catch (InstantiationException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (IllegalAccessException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		return list;
	}
	
	
	/**
	 * 小于：e.g:
	 * where :<60
	 */
	@Override
	public List<?> queryByLess(Class<?> claszz, String where,int indexdb) {
		// TODO 自动生成的方法存根
		String tableName=null;
		 List<Object> list=new ArrayList<Object>();
		 long now=new Date().getTime();
		try {
			boolean isExistTableName = claszz.newInstance().getClass().isAnnotationPresent(Table.class);
			if (isExistTableName) {
				Table pojo = (Table) claszz.newInstance().getClass().getAnnotation(Table.class);
			    if(pojo!=null) tableName=pojo.name();
			    Set<String>	sset= JedisUtil.getKeys(tableName+"*",indexdb);
			    if(sset!=null&&sset.size()>0){  	
			    	Set<String> obj_keys=JedisUtil.getIds(sset);
			    	for(String noid:obj_keys){
			    		String id=noid.substring(noid.indexOf(":")+1, noid.length());
			    		long t=(now -Long.parseLong(id))/1000;
			    		boolean key=(boolean)JedisUtil.Str2Express(t+where);
					 	if(key)list.add(this.get(claszz,noid,indexdb));
					  }
			    	//System.out.println("list-size:"+list.size());
			    } 
			}
		} catch (InstantiationException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (IllegalAccessException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		return list;
	}
	/**
	 * 小于：e.g:
	 * no:对象编号，不是记录id
	 * where :<60
	 */
	@Override
	public List<?> queryByNoLess(Class<?> claszz,String no, String where) {
		// TODO 自动生成的方法存根
		String tableName=null;
		 List<Object> list=new ArrayList<Object>();
		 long now=new Date().getTime();
		try {
			boolean isExistTableName = claszz.newInstance().getClass().isAnnotationPresent(Table.class);
			if (isExistTableName) {
				Table pojo = (Table) claszz.newInstance().getClass().getAnnotation(Table.class);
			    if(pojo!=null) tableName=pojo.name();
			    Set<String>	sset= JedisUtil.getKeys(tableName+":"+no+"*");
			    if(sset!=null&&sset.size()>0){  	
			    	Set<String> obj_keys=JedisUtil.getIds(sset);
			    	for(String noid:obj_keys){
			    		String id=noid.substring(noid.indexOf(":")+1, noid.length());
			    		long t=(now -Long.parseLong(id))/1000;
			    		boolean key=(boolean)JedisUtil.Str2Express(t+where);
					 	if(key)list.add(this.get(claszz,noid));
					  }
			    	//System.out.println("list-size:"+list.size());
			    } 
			}
		} catch (InstantiationException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (IllegalAccessException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		return list;
	}
	
	
	@Override
	public List<?> queryByNoLess(Class<?> claszz, String no, String where,int indexDB) {
		// TODO 自动生成的方法存根
		String tableName=null;
		 List<Object> list=new ArrayList<Object>();
		 long now=new Date().getTime();
		try {
			boolean isExistTableName = claszz.newInstance().getClass().isAnnotationPresent(Table.class);
			if (isExistTableName) {
				Table pojo = (Table) claszz.newInstance().getClass().getAnnotation(Table.class);
			    if(pojo!=null) tableName=pojo.name();
			    Set<String>	sset= JedisUtil.getKeys(tableName+":"+no+"*",indexDB);
			    if(sset!=null&&sset.size()>0){  	
			    	Set<String> obj_keys=JedisUtil.getIds(sset);
			    	for(String noid:obj_keys){
			    		String id=noid.substring(noid.indexOf(":")+1, noid.length());
			    		long t=(now -Long.parseLong(id))/1000;
			    		boolean key=(boolean)JedisUtil.Str2Express(t+where);
					 	if(key)list.add(this.get(claszz,noid,indexDB));
					  }
			    	//System.out.println("list-size:"+list.size());
			    } 
			}
		} catch (InstantiationException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (IllegalAccessException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		return list;
	}

	/**
	 * 选择数据要保存的数据库下标
	 */
	public  boolean save(Object obj,String userphone,int indexdb) {
		String tableName=null;
		String rowkey=null;
		boolean result=false;
	
		boolean isExistTableName = obj.getClass().isAnnotationPresent(Table.class);
		if (isExistTableName) {
			Table pojo = (Table)obj.getClass().getAnnotation(Table.class);
		    if(pojo!=null) tableName=pojo.name();
		     Class<?> clazz=  obj.getClass();
		     Field field[] =   clazz.getDeclaredFields();
		     int count=0;
		 	for (int i=0;i<field.length;i++) {
				Field f =field[i];
				try {
					 Key k= f.getAnnotation(Key.class);
					if(k!=null){
						Object value = invokeMethod(obj, f.getName(), null);
						 if(value!=null) 
							 rowkey=value.toString();
						 else 
							 rowkey=UUID.randomUUID().toString();
						 	
						 	String key=tableName+":"+userphone+":"+f.getName();
						 	boolean ff=JedisUtil.set(key, rowkey,indexdb);
						 	 if(ff) count++;
					}else{
						//按注解字段存
						//Colum col = f.getAnnotation(Colum.class);
						//String columName=col.columName();
						//String key=tableName+":"+userphone+":"+columName;
						String key=tableName+":"+userphone+":"+f.getName();
							Object value = invokeMethod(obj, f.getName(), null);
							String val="0";
							if(value==null){}else{val=value.toString();}
							boolean flag=JedisUtil.set(key, val,indexdb);
							if(flag) count++;
					}
				} catch (Exception e) {
					// TODO 自动生成的 catch 块
					e.printStackTrace();
				}
				
		 	}//for
		 	//System.out.println("count:"+count+"|field.length:"+field.length);
		 	result=	count==field.length ? true:false;
		}
		
		
		return result;
	}
	

	public String ClearDBData(int indexDB){
		return JedisUtil.ClearDBData(indexDB);
	}



	@Override
	public boolean setMsg(String key, String value) {
		// TODO Auto-generated method stub
		return JedisUtil._set(key, value,"string");
	}

	@Override
	public String getMsg(String key) {
		// TODO Auto-generated method stub
		return JedisUtil._get(key,"string");
	}
	


	@Override
	public boolean setObject(Class<?> clazz,String key, Object obj) {
		// TODO Auto-generated method stub
		ObjectsTranscoder<?> objTranscoder =  new ObjectsTranscoder<>();
		byte[] result1 = objTranscoder.serialize(obj);
		byte[] k=strToByteArray(key);
		return JedisUtil._set(k, result1,"table");
	}


	
	@Override
	public Object getObject(Class<?> clazz,String key) {
		// TODO Auto-generated method stub
		ObjectsTranscoder<?> objTranscoder =  new ObjectsTranscoder<>();
		byte[] k=strToByteArray(key);
		byte[] result1= JedisUtil._get(k,"table");
		return objTranscoder.deserialize(result1);
	}

	@Override
	public boolean setListBySQL(Class<?> clazz,String sqlkey, Object obj) {
		// TODO Auto-generated method stub
		ObjectsTranscoder<?> objTranscoder =  new ObjectsTranscoder<>();
		byte[] result1 = objTranscoder.serialize(obj);
		byte[] key=strToByteArray(sqlkey);
		return JedisUtil._set(key, result1,"list");
	}
	@Override
	public List<?> queryListBySQL(Class<?> clazz, String sqlkey) {
		// TODO Auto-generated method stub
		ObjectsTranscoder<?> objTranscoder =  new ObjectsTranscoder<>();
		byte[] key=strToByteArray(sqlkey);
		byte[] result1= JedisUtil._get(key,"list");
		return (List<?>) objTranscoder.deserialize(result1);
	}
	@Override
	public boolean setStrListBySQL(String sqlkey, List<?> list) {
		// TODO Auto-generated method stub
		return JedisUtil._set(sqlkey, List2Json.List2JsonString(list),"list");
	}
	@Override
	public <T>T queryStrListBySQL(Class<T> clazz, String sqlkey) {
		// TODO Auto-generated method stub
		return List2Json.JsonString2List(clazz, JedisUtil._get(sqlkey,"list"));
	}
	@Override
	public boolean setStrMapBySQL(String sqlkey, Map<String,String> map) {
		// TODO Auto-generated method stub
		return JedisUtil._set(sqlkey, List2Json.Map2String(map),"list");
	}
	@Override
	public Map queryStrMapBySQL(String sqlkey) {
		// TODO Auto-generated method stub
		return List2Json.String2Map(JedisUtil._get(sqlkey,"list"));
	}
	
	/*@Override
	public boolean addMapBySQL(Map<String,Object> map) {
		// TODO Auto-generated method stub
		String Str_key=null;
		int count=0;
		if(map!=null&&map.size()>0){
			if(map.size()==1){
				for(String key : map.keySet()){
					Str_key=key;
					break;
				}
				return	JedisUtil._set(Str_key,List2Json.Map2JsonString(map),"map");
			}else{
				for(String key : map.keySet()){
					boolean k=JedisUtil._set(key,List2Json.Map2JsonString(map),"map");
					if(k)count++;
				}
			}
			if(count==map.size()){
				return true;
			}
		}
		//return JedisUtil._set(Str_key,List2Json.Map2String(map),"map");
		return false;
	}
	
	@Override
	public Map<String,Object> findMapBySQL(String key) {
		return List2Json.parseJSON2Map(JedisUtil._get(key, "map"));
	}*/
	
	
	@Override
	public boolean setMapBySQL(String sqlkey, Map<String, ?> map) {
		// TODO Auto-generated method stub
		return JedisUtil._set(sqlkey, List2Json.Map2JsonString(map),"list");
	}
	
	@Override
	public Map<String,?> queryMapBySQL(String sqlkey) {
		// TODO Auto-generated method stub
		return List2Json.parseJSON2Map(JedisUtil._get(sqlkey,"list"));
	}
	
	@Override
	public boolean setListMapBySQL(String sqlkey, List<Map<String, Object>> list) {
		return JedisUtil._set(sqlkey, List2Json.listMap2JsonString(list),"list");
	}
	
	@Override
	public List<Map<String, Object>> queryListMapBySQL(String sqlkey) {
		return List2Json.jsonString2ListMap(JedisUtil._get(sqlkey,"list"));
	}
	
	/**
	 * 提前准备的同步操作
	 * 将表转换到redis中
	 * @param obj
	 * @return
	 */
	public  boolean syncSaveTable2Redis(Object obj,String type) {
		String tableName=null;
		String rowkey=null;
		boolean result=false;
	
		boolean isExistTableName = obj.getClass().isAnnotationPresent(Table.class);
		if (isExistTableName) {
			Table pojo = (Table)obj.getClass().getAnnotation(Table.class);
		    if(pojo!=null) tableName=pojo.name();
		     Class<?> clazz=  obj.getClass();
		     Field field[] =   clazz.getDeclaredFields();
		     int count=0;
		 	for (int i=0;i<field.length;i++) {
				Field f =field[i];
				try {
					 Key k= f.getAnnotation(Key.class);
					if(k!=null){
						Object value = invokeMethod(obj, f.getName(), null);
						 if(value!=null) 
							 rowkey=value.toString();
						 else 
							 rowkey=UUID.randomUUID().toString();
						 	
						 	String key=tableName+":"+rowkey+":"+f.getName();
						 	boolean ff=JedisUtil._set(key, rowkey,type);
						 	if(ff) count++;
					}else{
						//按注解字段存
						//Colum col = f.getAnnotation(Colum.class);
						//String columName=col.columName();
						//String key=tableName+":"+rowkey+":"+columName;
						String key=tableName+":"+rowkey+":"+f.getName();
							Object value = invokeMethod(obj, f.getName(), null);
							String val="";
							if(value!=null){val=value.toString();}
							boolean flag=JedisUtil._set(key, val,type);
							if(flag) count++;
					}
				} catch (Exception e) {
					// TODO 自动生成的 catch 块
					e.printStackTrace();
				}
				
		 	}//for
		 	result=	count==field.length ? true:false;
		}
		return result;
	}
	
	
	
	/**
	 * 提前准备的同步操作
	 * 将表转换到redis中
	 * @param obj
	 * @return
	 */
	public  boolean syncDeleteTable2Redis(Object obj) {
		String tableName=null;
		String rowkey=null;
		boolean result=false;
	
		boolean isExistTableName = obj.getClass().isAnnotationPresent(Table.class);
		if (isExistTableName) {
			Table pojo = (Table)obj.getClass().getAnnotation(Table.class);
		    if(pojo!=null) tableName=pojo.name();
		     Class<?> clazz=  obj.getClass();
		     Field field[] =   clazz.getDeclaredFields();
		     int count=0;
		 	for (int i=0;i<field.length;i++) {
				Field f =field[i];
				try {
					 Key k= f.getAnnotation(Key.class);
					if(k!=null){
						Object value = invokeMethod(obj, f.getName(), null);
						 if(value!=null) rowkey=value.toString();
						 	
						 	String key=tableName+":"+rowkey+":"+f.getName();
						 	boolean ff=JedisUtil.del(key);
						 	if(ff) count++;
					}else{
						//Colum col = f.getAnnotation(Colum.class);
						//String columName=col.columName();
						//String key=tableName+":"+rowkey+":"+columName;
						String key=tableName+":"+rowkey+":"+f.getName();
							boolean flag=JedisUtil.del(key);
							if(flag) count++;
					}
				} catch (Exception e) {
					// TODO 自动生成的 catch 块
					e.printStackTrace();
				}
				
		 	}//for
		 	result=	count==field.length ? true:false;
		}
		return result;
	}
	
	
	
	/**
	 * 提前准备的同步操作
	 * 将表转换到redis中
	 * @param obj
	 * @return
	 */
	public  boolean syncDeleteTable2Redis(Object obj,String type) {
		String tableName=null;
		String rowkey=null;
		boolean result=false;
	
		boolean isExistTableName = obj.getClass().isAnnotationPresent(Table.class);
		if (isExistTableName) {
			Table pojo = (Table)obj.getClass().getAnnotation(Table.class);
		    if(pojo!=null) tableName=pojo.name();
		     Class<?> clazz=  obj.getClass();
		     Field field[] =   clazz.getDeclaredFields();
		     int count=0;
		 	for (int i=0;i<field.length;i++) {
				Field f =field[i];
				try {
					 Key k= f.getAnnotation(Key.class);
					if(k!=null){
						Object value = invokeMethod(obj, f.getName(), null);
						 if(value!=null) rowkey=value.toString();
						 	
						 	String key=tableName+":"+rowkey+":"+f.getName();
						 	boolean ff=JedisUtil.del(key,type);
						 	if(ff) count++;
					}else{
						//Colum col = f.getAnnotation(Colum.class);
						//String columName=col.columName();
						//String key=tableName+":"+rowkey+":"+columName;
						String key=tableName+":"+rowkey+":"+f.getName();
							boolean flag=JedisUtil.del(key,type);
							if(flag) count++;
					}
				} catch (Exception e) {
					// TODO 自动生成的 catch 块
					e.printStackTrace();
				}
				
		 	}//for
		 	result=	count==field.length ? true:false;
		}
		return result;
	}
	
	
	/**
	 * 提前准备的同步操作
	 * 
	 */
	@Override
	public boolean syncUpdateTable2Redis(Object obj,String type) {
		// TODO Auto-generated method stub
		return syncSaveTable2Redis(obj,type);
	}
	
	/**
	 * 提前准备的同步操作
	 * 
	 */
	@Override
	public boolean syncDeleteTable2Redis(Class<?> obj,String type, String id) {
		// TODO Auto-generated method stub
		String tableName=null;
		boolean result=false;
	
		boolean isExistTableName = obj.getClass().isAnnotationPresent(Table.class);
		if (isExistTableName) {
			Table pojo = (Table)obj.getClass().getAnnotation(Table.class);
		    if(pojo!=null) tableName=pojo.name();
		     Class<?> clazz=  obj.getClass();
		     Field field[] =   clazz.getDeclaredFields();
		 	for (int i=0;i<field.length;i++) {
				Field f =field[i];
				try {
					 Key k= f.getAnnotation(Key.class);
					if(k!=null){
						String key=tableName+":"+id+":"+f.getName();
						result=JedisUtil.del(key,type);
						if(result)break;
					}
				} catch (Exception e) {
					// TODO 自动生成的 catch 块
					e.printStackTrace();
				}
				
		 	}//for
		}
		return result;
	}

	/**
	 * 提前准备的同步操作
	 * 
	 */
	@Override
	public boolean syncDeleteTable2Redis(Class<?> obj,String type, String... id) {
		// TODO Auto-generated method stub
		String tableName=null,tagId=null;
		boolean result=false;
	
		boolean isExistTableName = obj.getClass().isAnnotationPresent(Table.class);
		if (isExistTableName) {
			Table pojo = (Table)obj.getClass().getAnnotation(Table.class);
		    if(pojo!=null) tableName=pojo.name();
		     Class<?> clazz=  obj.getClass();
		     Field field[] =   clazz.getDeclaredFields();
		 	for (int i=0;i<field.length;i++) {
				Field f =field[i];
				try {
					 Key k= f.getAnnotation(Key.class);
					if(k!=null){
						tagId=f.getName();
						if(null!=tagId)break;
					}
				} catch (Exception e) {
					// TODO 自动生成的 catch 块
					e.printStackTrace();
				}
				
		 	}//for
		 	
		 	if(null!=tagId&&null!=id&&id.length>0){
		 		String[] key = new String[id.length];
		 		for(int i=0;i<id.length;i++){
		 			key[i]=tableName+":"+id[i]+":"+tagId;
		 		}
		 		result=JedisUtil.delKeys(type,key);
		 	}
		}
		return result;
	}
	
	
	
	@Override
	public boolean syncDeleteTable2Redis(Class<?> obj, String type, Integer id) {
		// TODO Auto-generated method stub
		String tableName=null;
		boolean result=false;
	
		boolean isExistTableName = obj.getClass().isAnnotationPresent(Table.class);
		if (isExistTableName) {
			Table pojo = (Table)obj.getClass().getAnnotation(Table.class);
		    if(pojo!=null) tableName=pojo.name();
		     Class<?> clazz=  obj.getClass();
		     Field field[] =   clazz.getDeclaredFields();
		 	for (int i=0;i<field.length;i++) {
				Field f =field[i];
				try {
					 Key k= f.getAnnotation(Key.class);
					if(k!=null){
						String key=tableName+":"+id+":"+f.getName();
						result=JedisUtil.del(key,type);
						if(result)break;
					}
				} catch (Exception e) {
					// TODO 自动生成的 catch 块
					e.printStackTrace();
				}
				
		 	}//for
		}
		return result;
	}
	
	
	@Override
	public boolean syncDeleteTable2Redis(Class<?> obj, String type,
			Integer... id) {
		String tableName=null,tagId=null;
		boolean result=false;
	
		boolean isExistTableName = obj.getClass().isAnnotationPresent(Table.class);
		if (isExistTableName) {
			Table pojo = (Table)obj.getClass().getAnnotation(Table.class);
		    if(pojo!=null) tableName=pojo.name();
		     Class<?> clazz=  obj.getClass();
		     Field field[] =   clazz.getDeclaredFields();
		 	for (int i=0;i<field.length;i++) {
				Field f =field[i];
				try {
					 Key k= f.getAnnotation(Key.class);
					if(k!=null){
						tagId=f.getName();
						if(null!=tagId)break;
					}
				} catch (Exception e) {
					// TODO 自动生成的 catch 块
					e.printStackTrace();
				}
				
		 	}//for
		 	
		 	if(null!=tagId&&null!=id&&id.length>0){
		 		String[] key = new String[id.length];
		 		for(int i=0;i<id.length;i++){
		 			key[i]=tableName+":"+id[i]+":"+tagId;
		 		}
		 		result=JedisUtil.delKeys(type,key);
		 	}
		}
		return result;
	}
	
	
	
	
	/**
	 * 提前准备的同步操作
	 */
	@Override
	public boolean syncUpdateStrListBySQL(Class<?> clazz, String sqlkey,List<?> list) {
		// TODO Auto-generated method stub
		return false;
	}
	
	/**
	 * 提前准备的同步操作
	 */
	@Override
	public boolean syncDeleteStrListBySQL(Class<?> clazz, String sqlkey) {
		// TODO Auto-generated method stub
		return false;
	}
	
	
	
	
	
	
}
