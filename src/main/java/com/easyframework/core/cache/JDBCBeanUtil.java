package com.easyframework.core.cache;

import java.lang.reflect.Field;
import java.lang.reflect.InvocationTargetException;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import javax.servlet.jsp.jstl.sql.Result;

import org.apache.commons.beanutils.BeanUtils;

import com.easyframework.core.a.Colum;
import com.easyframework.core.a.Table;


/**
 * 
 * 功能：将JDBC ResultSet、Result里的值转化为List
 * 当实例存在时，返回实例集合，当实例不存在时，返回对象数组集合
 * 时间 2011-04-21 上午09:43:35
 * 本次修改2017-06-23 13：09:22
 * 修改人：景林军
 */
public class JDBCBeanUtil{

	//private static Log logger = LogFactory.getLog(JDBCBeanUtil.class);
	
	/**
	 * 
	 */
	@SuppressWarnings("rawtypes")
	public static Object transTOObject(Result rs,Class<?> instance) throws SQLException, IllegalAccessException, InvocationTargetException, InstantiationException{
		Table t = (Table) instance.newInstance().getClass().getAnnotation(Table.class);
		String [] names=rs.getColumnNames();
	 	int count=names.length;
        Field[] fields={};
        Object[] objs=new Object[count];
        
        if(instance!=null){
        	fields=instance.getDeclaredFields();//取得实例对象的属性
        }  
        
        if(fields.length<1){//如果传入的对象当中没有属性
        	 Map row=rs.getRows()[0];//取得行
        	  for(int i=1;i<=count;i++){
        		  objs[i-1]=row.get(names[i]);//取得列的值 
        	  }
         return objs;  	//返回对象数组 
        }else{
        	//如果传入的对象当中有属性
        	Object obj=instance.newInstance();
        	 Map row=rs.getRows()[0];//取得行
         	   for(int i=0;i<count;i++){
                	for(int j=0;j<fields.length;j++){
                		Field field=fields[j];//依次取出实例对象的属性
                		//Object   o=row.get(names[j]);
                		field.setAccessible(true);// 开启私有变量的访问权限
						Colum colum = field.getAnnotation(Colum.class);
						String ty=null,colunName=null;
						if(null!=colum){
						 ty=colum.type().name().toUpperCase();
						 colunName=  colum.columName().toUpperCase();
						}
						
						if (null!=t&&t.type().toString().equals("TABLE")){
							if(names[i].contains("-")){
								names[i]=names[i].replaceAll("-","");
							}
							if(names[i].contains("_")){
								names[i]=names[i].replaceAll("_","");
							}
						}
						
                	//	logger.info("==>"+fields[j]+"======="+metaData.getColumnName(i).replaceAll("_",""));
						
					 if (null!=t&&null!=colunName){
						if (t.type().toString().equals("TABLE")&&field.getName().toUpperCase().equals(names[i].toUpperCase())){
                  			//BeanUtils.copyProperty(obj,field.getName(),row.get(names[i]));
							if(null!=ty&&ty.equals("Timestamp".toUpperCase())&&null!=row.get(names[i])){
								 String tmp=row.get(names[i]).toString();
							     tmp=tmp.substring(0,tmp.indexOf(".0"));
							     BeanUtils.copyProperty(obj,field.getName(), tmp);
							   }else{
								   BeanUtils.copyProperty(obj,field.getName(), row.get(names[i]));
							   }
                  			break;
                		}else
                		if (t.type().toString().equals("VO")&&colunName.equals(names[i].toUpperCase())){
                      		//BeanUtils.copyProperty(obj,field.getName(),row.get(names[i]));
                			if(ty.equals("Timestamp".toUpperCase())&&null!=row.get(names[i])){
								 String tmp=row.get(names[i]).toString();
							     tmp=tmp.substring(0,tmp.indexOf(".0"));
							     BeanUtils.copyProperty(obj,field.getName(), tmp);
							   }else{
								   BeanUtils.copyProperty(obj,field.getName(), row.get(names[i]));
							   }
                      		break;
                    	}
					 }else{
						 
						    String bsTmp=field.getName(),tmp=names[i];
						 		tmp=tmp.replaceAll("-","");
								tmp=tmp.replaceAll("_","");
							
								bsTmp=bsTmp.replaceAll("-","");
								bsTmp=bsTmp.replaceAll("_","");
								
							// if (null!=tmp&&null!=bsTmp){
								 if(bsTmp.toUpperCase().equals(tmp.toUpperCase())) {
									   BeanUtils.copyProperty(obj,field.getName(), row.get(names[i]));
									   break;
								 }
							// }
						/* if (field.getName().toUpperCase().equals(names[i].toUpperCase())){
							BeanUtils.copyProperty(obj,field.getName(), row.get(names[i]));
                 			break;
						 }*/
                    }
                  }
               } 
         	  return obj;   //返回实例对象
        }
        	
    }
	/**
	 * 
	 * @param r
	 * @param instance
	 * @return
	 * @throws SQLException
	 * @throws IllegalAccessException
	 * @throws InvocationTargetException
	 * @throws InstantiationException
	 */
	@SuppressWarnings("rawtypes")
	public static List<Object> transTOList(Result r,Class<?> instance) throws SQLException, IllegalAccessException, InvocationTargetException, InstantiationException{
		Table t = (Table) instance.newInstance().getClass().getAnnotation(Table.class);
		String [] names=r.getColumnNames();
		 	int c=names.length;
		 	int n=r.getRowCount();
		// System.out.println("c:"+c+"|n:"+n);
		
        Field[] fields={};
        if(instance!=null){
        	fields=instance.getDeclaredFields();//取得实例对象的属性
        }  
        //如果传入的对象当中没有属性
        if(fields.length<1){
        	List<Object> list=new ArrayList<Object>();
        	
          if(r!=null&&r.getRowCount()!=0){
        	  List<Object> arraylist=new ArrayList<Object>();
        	  for(int i=0;i<=n;i++){
        		  Map obj=r.getRows()[i];//取得行
        		  for(int j=0;j<=c;j++){
	        		  Object   o=obj.get(names[j]);    //取得列的值 
	        		  arraylist.add(o);
        		  }
        	  }
        	  list.add(arraylist.toArray());//把arraylist转化成数组以后，放进list
          }	
          return list;   
          //返回对象数组集合
        }else{
        	//如果传入的对象当中有属性
        	 List<Object> list=new ArrayList<Object>();
        	 for(int i=0;i<n;i++){
        		 Object newObj=instance.newInstance();//构造业务对象实例
                	 Map row=r.getRows()[i];//取得行
                	
                	for(int j=0;j<c;j++){
                		try {
							//Object   obj=row.get(names[j]); 
							Field field = fields[j];//依次取出实例对象的属性
							//System.out.println("f:" + field.getName()+ "|names:" + names[j]);
							field.setAccessible(true);// 开启私有变量的访问权限
							Colum colum = field.getAnnotation(Colum.class);
							String ty=null,colunName=null;
							if(null!=colum){
							 ty=colum.type().name().toUpperCase();
							 colunName=  colum.columName().toUpperCase();
							}
							String bs = field.getName();
							for (int s = 0; s < c; s++) {
								
								if (null!=t&&t.type().toString().equals("TABLE")){
									if(names[s].contains("-")){
										names[s]=names[s].replaceAll("-","");
									}
									if(names[s].contains("_")){
										names[s]=names[s].replaceAll("_","");
									}
								}
								
							if(null!=t&&null!=colunName){
								if (t.type().toString().equals("TABLE")&&bs.toUpperCase().equals(names[s].toUpperCase())) {
									if(null!=ty&&ty.equals("Timestamp".toUpperCase())&&null!=row.get(names[s])){
										 String tmp=row.get(names[s]).toString();
									     tmp=tmp.substring(0,tmp.indexOf(".0"));
									     BeanUtils.copyProperty(newObj,bs, tmp);
									   }else{
										   BeanUtils.copyProperty(newObj,bs, row.get(names[s]));
									   }
								}else
								 if (t.type().toString().equals("VO")&&colunName.equals(names[s].toUpperCase())) {
									
									 if(ty.equals("Timestamp".toUpperCase())&&null!=row.get(names[s])){
										 String tmp=row.get(names[s]).toString();
									     tmp=tmp.substring(0,tmp.indexOf(".0"));
									     BeanUtils.copyProperty(newObj,bs, tmp);
									   }else{
										   BeanUtils.copyProperty(newObj,bs, row.get(names[s]));
									   }
								}
							 }else{
								    String tmp=names[s];
								 		tmp=tmp.replaceAll("-","");
										tmp=tmp.replaceAll("_","");
									
										bs=bs.replaceAll("-","");
										bs=bs.replaceAll("_","");
										
										 if(bs.toUpperCase().equals(tmp.toUpperCase())) {
											   BeanUtils.copyProperty(newObj,bs, row.get(names[s]));
										 }
							}
						 }
						} catch (Exception e) {
							// TODO: handle exception
							e.fillInStackTrace();
							//logger.error("数据表查询结果转换封装对象列表异常："+e);
						}	
                	}
         	   list.add(newObj);
             }
        	//返回实例对象集合
        	return list;   
        }
	}
	
	
	/**
	 * 
	 * @param rs
	 * @param instance
	 * @return
	 * @throws SQLException
	 * @throws IllegalAccessException
	 * @throws InvocationTargetException
	 * @throws InstantiationException
	 */
	/*public static List<Object> transTurnList(ResultSet rs,Class<?> instance) throws SQLException, IllegalAccessException, InvocationTargetException, InstantiationException{
		ResultSetMetaData metaData=rs.getMetaData(); //取得结果集的元元素
		Table t = (Table) instance.newInstance().getClass().getAnnotation(Table.class);
        int count=metaData.getColumnCount(); //取得所有列的个数
        Field[] fields={};
        if(instance!=null){
        	fields=instance.getDeclaredFields();//取得实例对象的属性
        }  
        //如果传入的对象当中没有属性
        if(fields.length<1){
        	List<Object> list=new ArrayList<Object>();
        	
          while(rs.next()){
        	  List<Object> arraylist=new ArrayList<Object>();
        	  for(int i=1;i<=count;i++){
        		  Object obj=rs.getObject(i);//取得列的值 
        		  arraylist.add(obj);
        	  }
        	  list.add(arraylist.toArray());//把arraylist转化成数组以后，放进list
          }	
          return list;   
          //返回对象数组集合
        }else{
        	//如果传入的对象当中有属性
        	 List<Object> list=new ArrayList<Object>();
        	while(rs.next()){
        		 Object newInstance=instance.newInstance();//构造业务对象实例
         	   for(int i=1;i<=count;i++){
                	Object obj=rs.getObject(i);//取得列的值           	
                	for(int j=0;j<fields.length;j++){
                		Field field=fields[j];//依次取出实例对象的属性
                		field.setAccessible(true);// 开启私有变量的访问权限
						Colum colum = field.getAnnotation(Colum.class);
						String	colunName=null;
						if(null!=colum){	colunName=  colum.columName().toUpperCase();}
                		//field.getName().equalsIgnoreCase(metaData.getColumnName(i).replaceAll("_",""))
						String metanames =metaData.getColumnName(i);
                		if(metanames.contains("-")){
                			metanames=metanames.replaceAll("-","");
						}
						if(metanames.contains("_")){
							metanames=metanames.replaceAll("_","");
						}
                		if(null!=t&&t.type().toString().equals("TABLE")&&field.getName().equals(metanames.toUpperCase())){
                			BeanUtils.copyProperty(newInstance,field.getName(),obj);
                		}else
                		if(null!=t&&t.type().toString().equals("VO")&&null!=colunName&&colunName.equals(metanames.toUpperCase())){
                    			BeanUtils.copyProperty(newInstance,field.getName(),obj);
                    	}else{
                    		BeanUtils.copyProperty(newInstance,field.getName(),obj);
                    	}
                	}
                }  
         	   list.add(newInstance);
             }
        	//返回实例对象集合
        	return list;   
        }
	}*/
	/*public static Object transTurnObject(ResultSet rs,Class<?> instance) throws SQLException, IllegalAccessException, InvocationTargetException, InstantiationException{
		Table t = (Table) instance.newInstance().getClass().getAnnotation(Table.class);
		ResultSetMetaData metaData=rs.getMetaData(); //取得结果集的元元素
        int count=metaData.getColumnCount(); //取得所有列的个数
        Field[] fields={};
        Object[] objs=new Object[count];
        
        if(instance!=null){
        	fields=instance.getDeclaredFields();//取得实例对象的属性
        }  
        
        if(fields.length<1){//如果传入的对象当中没有属性
        	  for(int i=1;i<=count;i++){
        		  objs[i-1]=rs.getObject(i);//取得列的值 
        	  }
         return objs;  	//返回对象数组 
        }else{
        	//如果传入的对象当中有属性
        	Object obj=instance.newInstance();
        	while(rs.next()){
         	   for(int i=1;i<=count;i++){
                	for(int j=0;j<fields.length;j++){
                		Field field=fields[j];//依次取出实例对象的属性
                		field.setAccessible(true);// 开启私有变量的访问权限
						Colum colum = field.getAnnotation(Colum.class);
						String	colunName=null;
						if(null!=colum){	colunName=  colum.columName().toUpperCase();}
                		String metanames =metaData.getColumnName(i);
                		if(metanames.contains("-")){
                			metanames=metanames.replaceAll("-","");
						}
						if(metanames.contains("_")){
							metanames=metanames.replaceAll("_","");
						}
						
                	//	logger.info("==>"+fields[j]+"======="+metaData.getColumnName(i).replaceAll("_",""));
                		if(null!=t&&t.type().toString().equals("TABLE")&&field.getName().equals(metanames.toUpperCase())){
                  			BeanUtils.copyProperty(obj,field.getName(),rs.getObject(i));
                  			break;
                		}else
                		if(null!=t&&t.type().toString().equals("VO")&&null!=colunName&&colunName.equals(metanames.toUpperCase())){
                      			BeanUtils.copyProperty(obj,field.getName(),rs.getObject(i));
                      			break;
                    	}else{
                    		BeanUtils.copyProperty(obj,field.getName(),rs.getObject(i));
                  			break;
                    	}
                	}
               } 
        	}
         	  return obj;   //返回实例对象
        }
        	
    }
	*/
	
	
}
