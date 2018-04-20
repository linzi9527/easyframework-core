package com.easyframework.redis.core;

import java.lang.reflect.Field;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import javax.servlet.jsp.jstl.sql.Result;

import com.easyframework.core.a.Colum;
import com.easyframework.core.a.Table;
/**
 * 获取到的List<Map>结果集转化为JavaBean工具类
 * @author 蓝眼泪
 * 2018-2-9上午10:28:29
 * @param <T>
 */
public class BeanUtils<T> {

	/**
     * 根据List<Map<String, Object>>数据转换为JavaBean数据
     * @param datas 
     * @param beanClass
     * @return
     * @throws CommonException
     */
    public List<T> ListMap2JavaBean(List<Map<String, Object>> datas, Class<T> beanClass) throws CommonException  {
        // 返回数据集合
        List<T> list = null;
        // 对象字段名称
        String fieldname = "";
        // 对象方法名称
        String methodname = "";
        // 对象方法需要赋的值
        Object methodsetvalue = "";
        try {
            list = new ArrayList<T>();
            // 得到对象所有字段
            Field fields[] = beanClass.getDeclaredFields();
            Table ta = (Table) beanClass.newInstance().getClass().getAnnotation(Table.class);
            
            // 遍历数据
            for (Map<String, Object> mapdata : datas) {
                // 创建一个泛型类型实例
                T t = beanClass.newInstance();
                // 遍历所有字段，对应配置好的字段并赋值
                for (Field field : fields) {
                    if(null != field) { 
                    	
						field.setAccessible(true);// 开启私有变量的访问权限
						Colum colum = field.getAnnotation(Colum.class);
						String colunName=null;
						if(ta!=null&&null!=colum){
							colunName=  colum.columName();
							if (ta.type().toString().equals("TABLE")) {
		                        // 获取字段名称
		                        fieldname = colunName;
		                        // 拼接set方法
		                        methodname = "set" + StrUtil.capitalize(fieldname);
		                        // 获取data里的对应值
		                        methodsetvalue = mapdata.get(fieldname);
		                        // 赋值给字段
		                        Method m = beanClass.getDeclaredMethod(methodname, field.getType());
		                        m.invoke(t, methodsetvalue);
							}
							if (ta.type().toString().equals("VO")) {
		                        // 获取字段名称
		                        fieldname = field.getName();
		                        String key = colunName;
		                        // 拼接set方法
		                        methodname = "set" + StrUtil.capitalize(fieldname);
		                        // 获取data里的对应值
		                        methodsetvalue = mapdata.get(key);
		                        // 赋值给字段
		                        Method m = beanClass.getDeclaredMethod(methodname, field.getType());
		                        m.invoke(t, methodsetvalue);
							}
						}else{
	                    	// 全部转化为大写
	                    	//String dbfieldname = field.getName().toUpperCase();
	                        // 获取字段名称
	                        fieldname = field.getName();
	                        // 拼接set方法
	                        methodname = "set" + StrUtil.capitalize(fieldname);
	                        // 获取data里的对应值
	                        methodsetvalue = mapdata.get(fieldname);
	                        // 赋值给字段
	                        Method m = beanClass.getDeclaredMethod(methodname, field.getType());
	                        m.invoke(t, methodsetvalue);
						}
                    }
                }
                // 存入返回列表
                list.add(t);
            }
        } catch (InstantiationException e) {
            throw new CommonException(e, "创建beanClass实例异常");
        } catch (IllegalAccessException e) {
            throw new CommonException(e, "创建beanClass实例异常");
        } catch (SecurityException e) {
            throw new CommonException(e, "获取[" + fieldname + "] getter setter 方法异常");
        } catch (NoSuchMethodException e) {
            throw new CommonException(e, "获取[" + fieldname + "] getter setter 方法异常");
        } catch (IllegalArgumentException e) {
            throw new CommonException(e, "[" + methodname + "] 方法赋值异常");
        } catch (InvocationTargetException e) {
            throw new CommonException(e, "[" + methodname + "] 方法赋值异常");
        }
        // 返回
        return list;
    }
    

}
