package com.easyframework.redis.core;

import java.io.IOException;
import java.lang.reflect.Field;
import java.lang.reflect.Modifier;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.StringTokenizer;

import net.sf.json.JSONArray;
import net.sf.json.JSONObject;

import org.apache.commons.collections.map.ListOrderedMap;
import org.codehaus.jackson.JsonParseException;
import org.codehaus.jackson.map.JsonMappingException;
import org.codehaus.jackson.map.ObjectMapper;
import org.codehaus.jackson.map.SerializationConfig;
import org.codehaus.jackson.map.annotate.JsonSerialize;

public class List2Json {
	
    private static ObjectMapper objectMapper = new ObjectMapper();  
  
    static {  
        objectMapper.configure(SerializationConfig.Feature.WRITE_NULL_MAP_VALUES, false);  
        objectMapper.setSerializationInclusion(JsonSerialize.Inclusion.NON_NULL);  
    } 

	public static JSONArray List2Json(List<?> list){
         JSONArray json = JSONArray.fromObject(list);     
         return json;
    }
	
	public static String    List2JsonString(List<?> list){
        JSONArray json = JSONArray.fromObject(list);     
        return json.toString();
    }
	
	
	public static <T> T    JsonString2List(Class<T> clazz,String jsonStr){
		List<?> list = new ArrayList<>();   
		JSONArray jsonArray = JSONArray.fromObject(jsonStr);//把String转换为json   
		list = JSONArray.toList(jsonArray, clazz);//这里的t是Class<T>  
		return (T) list;
    }
	
	/** 
	 * 方法名称:transMap2String 
	 * 传入参数:map 
	 * 返回值:String 形如 username'chenziwen^password'1234 
	*/  
	public static String Map2String(Map map){  
	  java.util.Map.Entry entry;  
	  StringBuffer sb = new StringBuffer();  
	  for(Iterator iterator = map.entrySet().iterator(); iterator.hasNext();)  
	  {  
	    entry = (java.util.Map.Entry)iterator.next();  
	      sb.append(entry.getKey().toString()).append( "'" ).append(null==entry.getValue()?"":  
	      entry.getValue().toString()).append (iterator.hasNext() ? "^" : "");  
	  }  
	  return sb.toString();  
	}  
	
	
	/** 
	 * 方法名称:transString2Map 
	 * 传入参数:mapString 形如 username'chenziwen^password'1234 
	 * 返回值:Map 
	*/  
	public static Map String2Map(String mapString){  
	  Map map = new HashMap();  
	  java.util.StringTokenizer items;  
	  for(StringTokenizer entrys = new StringTokenizer(mapString, "^");entrys.hasMoreTokens();   
	    map.put(items.nextToken(), items.hasMoreTokens() ? ((Object) (items.nextToken())) : null))  
	      items = new StringTokenizer(entrys.nextToken(), "'");  
	  return map;  
	}  
	
	public static String Map2JsonString(Map<String,?> map){ 
		JSONObject json = JSONObject.fromObject(map); 
		 return json.toString();  
	}
	
    /** 
     * json字符串转Map 
     */  
    public static Map<String, ?> parseMap(String jsonStr) throws IOException {
    	//System.out.println("jsonStr:\n"+jsonStr);
        Map<String,?> map = objectMapper.readValue(jsonStr, Map.class);  
        return map;  
    } 
    public static Map<String, Object> parseJSON2Map(String jsonStr){
        ListOrderedMap map = new ListOrderedMap();
        //最外层解析
        JSONObject json = JSONObject.fromObject(jsonStr);
        for(Object k : json.keySet()){
            Object v = json.get(k); 
            //如果内层还是数组的话，继续解析
            if(v instanceof JSONArray){
                List<Map<String, Object>> list = new ArrayList<Map<String,Object>>();
                Iterator<JSONObject> it = ((JSONArray)v).iterator();
                while(it.hasNext()){
                    JSONObject json2 = it.next();
                    list.add(parseJSON2Map(json2.toString()));
                }
                map.put(k.toString(), list);
            } else {
                map.put(k.toString(), v);
            }
        }
        return map;
    }
    
    //----------利用commons.BeanUtils实现Obj和Map之间转换--------------------------------
    public static Object mapToObject(Map<String, Object> map, Class<?> beanClass)
            throws Exception {
        if (map == null)
            return null;
        Object obj = beanClass.newInstance();
        org.apache.commons.beanutils.BeanUtils.populate(obj, map);
        return obj;
    }

    public static Map<?, ?> objectToMap(Object obj) {
        if (obj == null) {
            return null;
        }
        return new org.apache.commons.beanutils.BeanMap(obj);
    }
    
    //----------利用java reflect完成Obj和Map之间的相互转换-----------------------
    public static Map<String,Object> Obj2Map(Object obj) throws Exception{
        Map<String,Object> map=new HashMap<String, Object>();
        Field[] fields = obj.getClass().getDeclaredFields();
        for(Field field:fields){
            field.setAccessible(true);
            map.put(field.getName(), field.get(obj));
        }
        return map;
    }
    public static Object map2Obj(Map<String,Object> map,Class<?> clz) throws Exception{
        Object obj = clz.newInstance();
        Field[] declaredFields = obj.getClass().getDeclaredFields();
        for(Field field:declaredFields){
            int mod = field.getModifiers(); 
            if(Modifier.isStatic(mod) || Modifier.isFinal(mod)){
                continue;
            }
            field.setAccessible(true);
            field.set(obj, map.get(field.getName()));
        }
        return obj;
    }
    
    
    public static String listMap2JsonString(List<Map<String, Object>> list){
    	StringBuffer sb=new StringBuffer();
    	int count=0;
    	sb.append("[");
    	for (Map<String, Object> m : list)
        {
    		if(count==(list.size()-1)){
    			sb.append(Map2JsonString(m));
    		}else{
    			sb.append(Map2JsonString(m)).append(",");
    		}
    		count++;
    		
        }
    	sb.append("]");
    	return sb.toString();
    }

    public static List<Map<String, Object>> jsonString2ListMap(String jsonStr) {
    	List<Map<String, Object>> list=null;
		try {
			list = objectMapper.readValue(jsonStr, List.class);
		} catch (JsonParseException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (JsonMappingException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
    	return list;
    }
    
    
    public static List<Object> jsonString2List(String jsonStr) {
    	List<Object> list=null;
		try {
			list = objectMapper.readValue(jsonStr, List.class);
		} catch (JsonParseException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (JsonMappingException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
    	return list;
    }
    /*
    * list转map
    * <p>
    * Map<String,List<String>>
    */
    public static Map<String, List<String>> convert(List<Map<String, Object>> list) {
       // 这是要返回的map对象
       HashMap<String, List<String>> map = new HashMap<>();

       // 对应的list
       ArrayList<String> mapList = new ArrayList<>();
       String key = null;
       // 遍历list
       for (Map<String, Object> item : list) {
           // 取出item中所有对应的value
           for (String itemKey : item.keySet()) {
               // 因为key都是xxx，所以直接去第一个map的可以就可以了
               if (key == null) {
                   key = itemKey;
               }
               mapList.add(String.valueOf(item.get(itemKey)));
           }
       }
       // 遍历结束
       map.put(key, mapList);
       return map;
   }
    public static void main(String[] args) throws JsonParseException, JsonMappingException, IOException {

        List<Map<String, Object>> listMaps = new ArrayList<Map<String, Object>>();
        Map<String, Object> map1 = new HashMap<String, Object>();
        map1.put("id", "11111111");
        map1.put("username", "linzi");
        map1.put("phone", "133870980932");
        listMaps.add(map1);

        Map<String, Object> map2 = new HashMap<String, Object>();
        map2.put("id", "2222222");
        map2.put("username", "mysql");
        map2.put("phone", "153870980955");
        listMaps.add(map2);
        String json= listMap2JsonString(listMaps);
        System.out.println(json);
        System.out.println("=====================================================");
        
        List<Map<String, Object>> list=jsonString2ListMap(json);
        for (Map<String, Object> m : list)
        {
          for (String k : m.keySet())
          {
            System.out.println(k + " : " + m.get(k));
          }

        }
      

    }

}
