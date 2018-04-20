package com.easyframework.core.db;

import java.io.UnsupportedEncodingException;
import java.lang.reflect.Field;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import javax.servlet.jsp.jstl.sql.Result;
import javax.servlet.jsp.jstl.sql.ResultSupport;

import org.apache.log4j.Logger;

import com.easyframework.core.a.Colum;
import com.easyframework.core.a.Key;
import com.easyframework.core.a.Table;
import com.easyframework.core.cache.Command;
import com.easyframework.core.cache.JDBCBeanUtil;
import com.easyframework.core.cache.SqlCommonDAO;

@SuppressWarnings("rawtypes")
public class SlaveDBHelper_v2 {
	private static final Logger log = Logger.getLogger(SlaveDBHelper_v2.class);
	
	
	private static final SlaveDBHelper_v2 postDB = new SlaveDBHelper_v2();
	private Connection connection = null;
	private String[] link_name = null;

	public static SlaveDBHelper_v2 getInstance() {
		return postDB;
	}

	/**
	 * 设置关联表名称即表名
	 */
	public SlaveDBHelper_v2 setTableLink(String[] tbl_name) {
		if (!StringUtil.isNull(tbl_name)) {
			link_name = tbl_name;
		}
		return this;
	}

	

	/**
	 * 查询数据
	 * 
	 * @param sql
	 * @return
	 */
	@SuppressWarnings("static-access")
	public ResultSet executeQuery(String sql) {
		ResultSet result = null;
		PreparedStatement preparedStatement;
		try {
			if (connection == null || connection.isClosed()) {
				connection = SlaveConnectionFactory.getInstance().getConnection();
				// connection=SlaveConnectionFactory.getConnection();
			}
			preparedStatement = connection.prepareStatement(sql);
			result = preparedStatement.executeQuery();

		} catch (SQLException e) {
			log.error("executeQuery-->" + e);
		}
		return result;
	}

	/**
	 * 查询实体集
	 * 
	 * @param o
	 * @return
	 */
	@SuppressWarnings("static-access")
	public List<?> find(Class<?> o, boolean isCache) {
		SqlCommonDAO helper = new SqlCommonDAO();
		StringBuffer sb = new StringBuffer();
		List list = null;
		sb.append("SELECT * FROM ");
		String table = "";
		try {

			if (connection == null || connection.isClosed()) {
				// connection=SlaveConnectionFactory.getConnection();
				connection =SlaveConnectionFactory.getInstance().getConnection();
			}
			boolean tbl = o.newInstance().getClass().isAnnotationPresent(Table.class);
			//System.out.println("isTable:" + tbl);
			if (tbl) {
				Table t = (Table) o.newInstance().getClass().getAnnotation(Table.class);
				// table=connection.getCatalog()+"."+t.name();
				table = t.name();
				sb.append(table);
			}


			helper.setConnection(connection);
			Command c = new Command();
			helper.setCommand(c);
			c.setCache(isCache);
			c.setTables(new String[] { table });
			c.setSql(sb.toString());
			// System.out.println(sb.toString());
			Result rs = helper.executeQuery();
			if(rs!=null&&rs.getRowCount()>0) list = JDBCBeanUtil.transTOList(rs, o);
			//connection.close();
		} catch (SQLException e) {
			// TODO Auto-generated catch block
			//e.printStackTrace();
			log.error("SQLException-->" + e.getMessage());
		} catch (IllegalAccessException e) {
			// TODO Auto-generated catch block
		    //e.printStackTrace();
			log.error("IllegalAccessException-->" + e.getMessage());
		} catch (InvocationTargetException e) {
			// TODO Auto-generated catch block
			//e.printStackTrace();
			log.error("InvocationTargetException-->" + e.getMessage());
		} catch (InstantiationException e) {
			// TODO Auto-generated catch block
			//e.printStackTrace();
			log.error("InstantiationException-->" + e.getMessage());
		} finally {
			helper.closeConnection(connection);
		}
		return list;
	}

	@SuppressWarnings("static-access")
	public static List<?> querys(Class<?> o, boolean isCache) throws InvocationTargetException {
		SqlCommonDAO helper = new SqlCommonDAO();
		StringBuffer sb = new StringBuffer();
		List<?> list = null;
		Connection connection=null;
		sb.append("SELECT * FROM ");
		String table = "";
		try {

			if (connection == null || connection.isClosed()) {
				connection = SlaveConnectionFactory.getInstance().getConnection();
			}
			boolean tbl = o.newInstance().getClass().isAnnotationPresent(Table.class);
			//System.out.println("isTable:" + tbl);
			if (tbl) {
				Table t = (Table) o.newInstance().getClass().getAnnotation(Table.class);
				//Package p=o.newInstance().getClass().getPackage();
				String cname=o.newInstance().getClass().getName();
				// table=connection.getCatalog()+"."+t.name();
				table = t.name();
				//System.out.println("table："+table+"|cname:"+cname);
				sb.append(table);
			
			helper.setConnection(connection);
			Command c = new Command();
			c.setCache(isCache);
			c.setPakage_clazz(cname);
			c.setTables(new String[] { table });
			c.setSql(sb.toString());
			helper.setCommand(c);
			Result rs = helper.executeQueryByCache();
			if(rs!=null&&rs.getRowCount()>0) list = JDBCBeanUtil.transTOList(rs, o);
			//connection.close();
		  }else{
			  System.out.println("数据VO没加注解");
		  }
		} catch (SQLException e) {
			// TODO Auto-generated catch block
			//e.printStackTrace();
			log.error("SQLException-->" + e.getMessage());
		} catch (IllegalAccessException e) {
			// TODO Auto-generated catch block
		    //e.printStackTrace();
			log.error("IllegalAccessException-->" + e.getMessage());
		} catch (InvocationTargetException e) {
			// TODO Auto-generated catch block
			//e.printStackTrace();
			log.error("InvocationTargetException-->" + e.getMessage());
		} catch (InstantiationException e) {
			// TODO Auto-generated catch block
			//e.printStackTrace();
			log.error("InstantiationException-->" + e.getMessage());
		} finally {
			helper.closeConnection(connection);
		}
		return list;
	}
/**
 * 查询实体列表清单
 * @param o :实体对象Obj.class
 * @param isWhere : where id=id and ...
 * @param group  : group by ...
 * @param order  : order by ...
 * @param isCache : true or false 是否开启查询缓存
 * @return
 */
	@SuppressWarnings("static-access")
	public List<?> queryList(Class<?> o, String isWhere, boolean isCache) {
		SqlCommonDAO helper = new SqlCommonDAO();
		StringBuffer sb = new StringBuffer();
		List list = null;
		sb.append("SELECT * FROM ");
		String table = "";
		try {

			if (connection == null || connection.isClosed()) {
				// connection=SlaveSlaveConnectionFactory.getConnection();
				connection = SlaveConnectionFactory.getInstance().getConnection();
			}
			boolean tbl = o.newInstance().getClass().isAnnotationPresent(Table.class);
			//System.out.println("isTable:" + tbl);
			if (tbl) {
				Table t = (Table) o.newInstance().getClass().getAnnotation(Table.class);
				//table = connection.getCatalog() + "." + t.name();
				table =t.name();
				String n = t.name().substring(t.name().indexOf("_") + 1, t.name().indexOf("_") + 2).toLowerCase();
				sb.append(table).append(" " + n + "  ");
			}

			if (!StringUtil.isNull(isWhere)) {
				sb.append(isWhere);
			}
			

			helper.setConnection(connection);
			Command c = new Command();
			helper.setCommand(c);
			c.setCache(isCache);
			c.setTables(new String[] { table });
			c.setSql(sb.toString());
			// System.out.println(sb.toString());
			Result rs = helper.executeQuery();
			if(rs!=null&&rs.getRowCount()>0) list = JDBCBeanUtil.transTOList(rs, o);
			//connection.close();
		} catch (SQLException e) {
			// TODO Auto-generated catch block
			//e.printStackTrace();
			log.error("SQLException-->" + e.getMessage());
		} catch (IllegalAccessException e) {
			// TODO Auto-generated catch block
		    //e.printStackTrace();
			log.error("IllegalAccessException-->" + e.getMessage());
		} catch (InvocationTargetException e) {
			// TODO Auto-generated catch block
			//e.printStackTrace();
			log.error("InvocationTargetException-->" + e.getMessage());
		} catch (InstantiationException e) {
			// TODO Auto-generated catch block
			//e.printStackTrace();
			log.error("InstantiationException-->" + e.getMessage());
		}finally {
			helper.closeConnection(connection);
		}
		return list;
	}

	
	@SuppressWarnings("static-access")
	public List<?> query(Class<?> o, String sql, boolean isCache) {
		List list = null;
		SqlCommonDAO helper = new SqlCommonDAO();
		try {
			if (connection == null || connection.isClosed()) {
				// connection=SlaveConnectionFactory.getConnection();
				connection = SlaveConnectionFactory.getInstance().getConnection();
			}
			if (!StringUtil.isNull(sql)) {
				helper.setConnection(connection);
				Command c = new Command();
				helper.setCommand(c);
				c.setCache(isCache);
				c.setTables(link_name);
				c.setSql(sql);

				Result rs = helper.executeQuery();
				if(rs!=null&&rs.getRowCount()>0) list = JDBCBeanUtil.transTOList(rs, o);
			}
			//connection.close();
		} catch (SQLException e) {
			// TODO Auto-generated catch block
			//e.printStackTrace();
			log.error("SQLException-->" + e.getMessage());
		} catch (IllegalAccessException e) {
			// TODO Auto-generated catch block
		    //e.printStackTrace();
			log.error("IllegalAccessException-->" + e.getMessage());
		} catch (InvocationTargetException e) {
			// TODO Auto-generated catch block
			//e.printStackTrace();
			log.error("InvocationTargetException-->" + e.getMessage());
		} catch (InstantiationException e) {
			// TODO Auto-generated catch block
			//e.printStackTrace();
			log.error("InstantiationException-->" + e.getMessage());
		}finally {
			helper.closeConnection(connection);
		}
		return list;
	}
	
	

	/**
	 * 自定义多表联合查询sql语句 必须设置那几张表关联
	 * setTableLink 
	 * e.g:
	 * String sql=
	 *   "SELECT t0.id,t0.user_id,t0.user_name,t0.address,t1.emp_Name,t1.emp_Pwd,t1.emp_Addr FROM tbl_user AS t0 right JOIN tbl_employee AS t1 ON t0.id=t1.emp_user_no";
	 *  调用形式： postDB.query(UEVO.class,sql,false);
	 * 
	 * @param o
	 * @param sql
	 * @return
	 */
	@SuppressWarnings("static-access")
	public List<?> queryTables(Class<?> o, String [] link_name,String sql, boolean isCache) {
		List list = null;
		SqlCommonDAO helper = new SqlCommonDAO();
		try {
			if (connection == null || connection.isClosed()) {
				// connection=SlaveConnectionFactory.getConnection();
				connection = SlaveConnectionFactory.getInstance().getConnection();
			}
			if (!StringUtil.isNull(sql)) {
				helper.setConnection(connection);
				Command c = new Command();
				helper.setCommand(c);
				c.setCache(isCache);
				c.setTables(link_name);
				c.setSql(sql);

				Result rs = helper.executeQuery();
				if(rs!=null&&rs.getRowCount()>0) list = JDBCBeanUtil.transTOList(rs, o);
			}
			//connection.close();
		} catch (SQLException e) {
			// TODO Auto-generated catch block
			//e.printStackTrace();
			log.error("SQLException-->" + e.getMessage());
		} catch (IllegalAccessException e) {
			// TODO Auto-generated catch block
		    //e.printStackTrace();
			log.error("IllegalAccessException-->" + e.getMessage());
		} catch (InvocationTargetException e) {
			// TODO Auto-generated catch block
			//e.printStackTrace();
			log.error("InvocationTargetException-->" + e.getMessage());
		} catch (InstantiationException e) {
			// TODO Auto-generated catch block
			//e.printStackTrace();
			log.error("InstantiationException-->" + e.getMessage());
		}finally {
			helper.closeConnection(connection);
		}
		return list;
	}

	/**
	 * 通用添加实体对象
	 * @throws Exception 
	 */
	@SuppressWarnings("static-access")
	public boolean save(Object obj) throws Exception {
		int i = 0;
		String tbl_name = "";
		String nameColum = "",valueColum="";// 记录字段名
		Key	k =null;
		StringBuffer sb = new StringBuffer();
		StringBuffer name = new StringBuffer();
		StringBuffer value = new StringBuffer();
		try {
			try {
				if (connection == null || connection.isClosed()) {
					connection = SlaveConnectionFactory.getInstance().getConnection();
				}
			} catch (SQLException e1) {
				// TODO Auto-generated catch block
				e1.printStackTrace();
			}
			
			boolean tbl = obj.getClass().isAnnotationPresent(Table.class);
			sb.append("INSERT INTO ");
			if (tbl) {
				Table t = (Table) obj.getClass().getAnnotation(Table.class);
				tbl_name =t.name();
				sb.append(t.name());
				
				Field[] fields = obj.getClass().getDeclaredFields();
				if(fields.length>1){
					for(Field f:fields){
						f.setAccessible(true);// 开启私有变量的访问权限
						Colum c = f.getAnnotation(Colum.class);
						Object v=f.get(obj);
						String tp=c.type().name().toUpperCase();
						boolean flag = f.isAnnotationPresent(Key.class);
						if (flag){
							k = f.getAnnotation(Key.class);
							if(k.isPrimary()&&k.isAuto()&&(tp.endsWith("INT")||tp.endsWith("INTEGER"))){
								name.append(c.columName()).append(",");
								value.append("null,");
							}else if(k.isPrimary()&&(tp.endsWith("VARCHAR")||tp.endsWith("STRING"))){
								name.append(c.columName()).append(",");
								value.append("'"+v+"'").append(",");
							}
							
						}else{
							if(tp.endsWith("INT")||tp.endsWith("INTEGER")){
								name.append(c.columName()).append(",");
								value.append(v).append(",");
							}else
							if(tp.endsWith("VARCHAR")||tp.endsWith("STRING")){
								name.append(c.columName()).append(",");
								value.append("'"+v+"'").append(",");
							}else	
							if(tp.endsWith("DATE")){
								name.append(c.columName()).append(",");
								value.append("CURDATE(),");
							}else
							if(tp.endsWith("Timestamp".toUpperCase())){
								name.append(c.columName()).append(",");
								value.append("NOW(),");
							}else	
							if(tp.endsWith("BOOLEAN")||tp.endsWith("LONG")){
								name.append(c.columName()).append(",");
								value.append(v).append(",");
							}else
							if(tp.endsWith("DOUBLE")||tp.endsWith("FLOAT")){
								name.append(c.columName()).append(",");
								value.append(v).append(",");
							}else{
								name.append(c.columName()).append(",");
								value.append("'"+v+"'").append(",");
							}
						}
					}
					
					nameColum=name.toString().substring(0, name.toString().length()-1);
					valueColum=value.toString().substring(0, value.toString().length()-1);
					sb.append("(").append(nameColum).append(")")
					  .append(" VALUES (").append(valueColum).append(")");
				}else{
					log.info("===Bean中没有属性===");
				}
			}else{
				log.error("对应VO没有与操作表形成映射，出现异常！");
				return false;
			}

		
			SqlCommonDAO helper = new SqlCommonDAO();
				try {
					helper.setConnection(connection);
					Command c = new Command();
					helper.setCommand(c);
					c.setCache(false);
					c.setTables(new String[] { tbl_name });
					c.setSql(sb.toString());
					i = helper.executeUpdate();
				} catch (SQLException e) {
					// TODO Auto-generated catch block
					log.error("入库使用缓存异常："+sb.toString(),e);
				} finally {
					helper.closeConnection(connection);
				}


		} catch (IllegalAccessException e) {
			// TODO Auto-generated catch block
			//e.printStackTrace();
			log.error("入库异常："+sb.toString(),e);
		}
		return i > 0 ? true : false;

	}// end
	
	/**
	 * 通用添加实体对象,存在就更新，不存在就插入
	 * @throws Exception 
	 */
	public boolean saveUpdate(Object obj) throws Exception {
		Key	k =null;
		boolean flag=false;
		ResultSet rs=null;
		StringBuffer sb = new StringBuffer();
		StringBuffer name = new StringBuffer();
		StringBuffer value = new StringBuffer();
		
		StringBuffer sb_isExist = new StringBuffer();
		sb_isExist.append("SELECT count(*) FROM ");
		try {
			
			boolean tbl = obj.getClass().isAnnotationPresent(Table.class);
			if (tbl) {
				Table t = (Table) obj.getClass().getAnnotation(Table.class);
				sb_isExist.append(t.name());
				sb_isExist.append(" WHERE ");
				Field[] fields = obj.getClass().getDeclaredFields();
				if(fields.length>1){
					for(Field f:fields){
						f.setAccessible(true);// 开启私有变量的访问权限
						Colum c = f.getAnnotation(Colum.class);
						Object v=f.get(obj);
						String tp=c.type().name().toUpperCase();
						if (f.isAnnotationPresent(Key.class)){
							k = f.getAnnotation(Key.class);
							if(k.isPrimary()&&k.isAuto()&&(tp.endsWith("INT")||tp.endsWith("INTEGER"))){
								name.append(c.columName()).append(",");
								value.append("null,");
								sb_isExist.append(c.columName());
								sb_isExist.append("=");
								sb_isExist.append(v);
							}else if(k.isPrimary()&&(tp.endsWith("VARCHAR")||tp.endsWith("STRING"))){
								name.append(c.columName()).append(",");
								value.append("'"+v+"'").append(",");
								sb_isExist.append(c.columName());
								sb_isExist.append("=");
								sb_isExist.append("'").append(v).append("' ");
							}
							
						}
					}
					
				}else{
					log.info("===Bean中没有属性===");
				}
			}else{
				log.error("对应VO没有与操作表形成映射，出现异常！");
				return false;
			}

		
				try {
					boolean key=false;
					 rs=this.executeQuery(sb_isExist.toString());
					log.info("isExist-SQL:"+sb_isExist.toString());
					if(rs!=null){
						while(rs.next()){
							key=rs.getInt(1)>0?true:false;
						}
						if(key)flag=this.update(obj);
						else flag=this.save(obj);
					}
				} catch (SQLException e) {
					// TODO Auto-generated catch block
					log.error("入库使用缓存异常："+sb.toString(),e);
				} finally{
					rs.close();
				}


		} catch (IllegalAccessException e) {
			log.error("入库异常："+e.getMessage());
		}
		return flag;

	}// end
	/**
	 * 通用添加实体对象,返回新增对象ID
	 * @throws Exception 
	 */
	@SuppressWarnings("static-access")
	public String saveForKey(Object obj) throws Exception {
		//int i = 0;
		String newID =null;
		String nameColum = "",valueColum="";// 记录字段名
		Key	k =null;
		StringBuffer sb = new StringBuffer();
		StringBuffer name = new StringBuffer();
		StringBuffer value = new StringBuffer();
		try {
			try {
				if (connection == null || connection.isClosed()) {
					connection = SlaveConnectionFactory.getInstance().getConnection();
				}
			} catch (SQLException e1) {
				// TODO Auto-generated catch block
				e1.printStackTrace();
			}
			
			boolean tbl = obj.getClass().isAnnotationPresent(Table.class);
			sb.append("INSERT INTO ");
			if (tbl) {
				Table t = (Table) obj.getClass().getAnnotation(Table.class);
				//tbl_name =t.name();
				sb.append(t.name());
				
				Field[] fields = obj.getClass().getDeclaredFields();
				if(fields.length>1){
					for(Field f:fields){
						f.setAccessible(true);// 开启私有变量的访问权限
						Colum c = f.getAnnotation(Colum.class);
						Object v=f.get(obj);
						String tp=c.type().name().toUpperCase();
						boolean flag = f.isAnnotationPresent(Key.class);
						if (flag){
							k = f.getAnnotation(Key.class);
							if(k.isPrimary()&&k.isAuto()&&(tp.endsWith("INT")||tp.endsWith("INTEGER"))){
								name.append(c.columName()).append(",");
								value.append("null,");
							}else if(k.isPrimary()&&(tp.endsWith("VARCHAR")||tp.endsWith("STRING"))){
								name.append(c.columName()).append(",");
								value.append("'"+v+"'").append(",");
							}
							
						}else{
							if(tp.endsWith("INT")||tp.endsWith("INTEGER")){
								name.append(c.columName()).append(",");
								value.append(v).append(",");
							}else
							if(tp.endsWith("VARCHAR")||tp.endsWith("STRING")){
								name.append(c.columName()).append(",");
								value.append("'"+v+"'").append(",");
							}else	
							if(tp.endsWith("DATE")){
								name.append(c.columName()).append(",");
								value.append("CURDATE(),");
							}else
							if(tp.endsWith("Timestamp")){
								name.append(c.columName()).append(",");
								value.append("NOW(),");
							}else	
							if(tp.endsWith("BOOLEAN")||tp.endsWith("LONG")){
								name.append(c.columName()).append(",");
								value.append(v).append(",");
							}else
							if(tp.endsWith("DOUBLE")||tp.endsWith("FLOAT")){
								name.append(c.columName()).append(",");
								value.append(v).append(",");
							}else{
								name.append(c.columName()).append(",");
								value.append("'"+v+"'").append(",");
							}
						}
					}
					
					nameColum=name.toString().substring(0, name.toString().length()-1);
					valueColum=value.toString().substring(0, value.toString().length()-1);
					sb.append("(").append(nameColum).append(")")
					  .append(" VALUES (").append(valueColum).append(")");
				}else{
					log.info("===vo中没有属性===");
				}
			}else{
				log.error("对应VO没有与操作表形成映射，出现异常！");
				//return false;
			}

		
			SqlCommonDAO helper = new SqlCommonDAO();
				try {
					helper.setConnection(connection);
					/*Command c = new Command();
					helper.setCommand(c);
					c.setCache(false);
					c.setTables(new String[] { tbl_name });
					c.setSql(sb.toString());*/
					newID= helper.executeUpdateForKey(sb.toString(),connection);
				} catch (SQLException e) {
					// TODO Auto-generated catch block
					log.error("入库使用缓存异常："+sb.toString(),e);
				} finally {
					helper.closeConnection(connection);
				}


		} catch (IllegalAccessException e) {
			// TODO Auto-generated catch block
			//e.printStackTrace();
			log.error("入库异常："+sb.toString(),e);
		}
		return newID;

	}// end

	

	/**
	 * 更新实体对象
	 * 
	 * @param obj
	 * @return
	 * @throws IllegalArgumentException
	 * @throws IllegalAccessException
	 * @throws MyException
	 */
	@SuppressWarnings({ "static-access"})
	public boolean update(Object obj) {
		String wh = "";// 记录字段名
		String k = "";
		boolean bl = false;// 记录主键是否为空
		boolean flag = false;
		int i = 0;
		Key isKey = null;
		String tbl_name = "";
		String sql = "UPDATE ";
		try {
			try {
				if (connection == null || connection.isClosed()) {
					connection = SlaveConnectionFactory.getInstance().getConnection();
				}
			} catch (SQLException e1) {
				// TODO Auto-generated catch block
				e1.printStackTrace();
			}
			if (obj != null) {
				Field[] fiels = obj.getClass().getDeclaredFields();// 获得反射对象集合
				boolean t = obj.getClass().isAnnotationPresent(Table.class);// 获得类是否有注解
				//System.out.println("isTable:" + t);
				if (t) {
					Table tab = obj.getClass().getAnnotation(Table.class);
						tbl_name =  tab.name();
						sql += tbl_name + "  SET  ";// 获得表名

					for (Field fl : fiels) {// 循环组装
						fl.setAccessible(true);// 开启支私有变量的访问权限
						Object tobj = fl.get(obj);
						Colum c = fl.getAnnotation(Colum.class);
						flag = fl.isAnnotationPresent(Key.class);
						if (flag)
							isKey = fl.getAnnotation(Key.class);
						String tp=fl.getType().getName().toLowerCase();
						if (tobj != null && isKey != null) {
							if (!bl && isKey.isPrimary()) {// 判断是否存在主键
								bl = true;
								if (tp.endsWith("string"))
									k = "  WHERE  " + fl.getName() + "='" + tobj.toString() + "'  ";
								else
									k = "  WHERE  " + fl.getName() + "=" + tobj.toString() + "  ";

							} 
							else {
								if(c != null &&tp.endsWith("date")){
									if(tobj!=null){
										wh += fl.getName() + "=CURDATE(),";
									}
								}else if(c != null &&tp.endsWith("timestamp")){
									if(tobj!=null){
										wh += fl.getName() + "=NOW(),";
									 }
								}else if (c != null && tp.equals(c.columName().toLowerCase())) {
									if (tp.endsWith("string"))
										wh += fl.getName() + "='" + tobj.toString() + "',";
									else
										wh += fl.getName() + "=" + tobj.toString() + ",";
								} else {
									if (tp.endsWith("string"))
										wh += c.columName() + "='" + tobj.toString() + "',";
									else
										wh += c.columName() + "=" + tobj.toString() + ",";
								}
							}
						}
					}
					if (wh.length() > 0 && k.length() > 0) {
						
						wh = wh.substring(0, wh.lastIndexOf(","));
						k = k.substring(0, k.length() - 1);
						sql += wh + k;
						//System.out.println(sql);
						SqlCommonDAO helper = new SqlCommonDAO();
						try {
							helper.setConnection(connection);
							Command c = new Command();
							helper.setCommand(c);
							c.setCache(false);
							c.setTables(new String[] { tbl_name });
							c.setSql(sql.toString());
							i = helper.executeUpdate();
						} catch (SQLException e) {
							// TODO Auto-generated catch block
							log.error("删除操作异常sql:"+sql,e);
							//e.printStackTrace();
						} finally {
							helper.closeConnection(connection);
						}
					}
					// log.info("\n"+sql);
				}
			}
		} catch (IllegalArgumentException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (IllegalAccessException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}

		return i > 0 ? true : false;
	}

	/**
	 * 根据实体Object删除记录
	 * 
	 * @param id
	 * @return
	 */
	@SuppressWarnings("static-access")
	public boolean remove(Object obj) {
		boolean flag = false;
		boolean flag_is = false;
		Key isKey = null;
		String tbl_name = "";
		String sql = "DELETE FROM  ";
		int i = 0;
		try {
			if (connection == null || connection.isClosed()) {
				connection = SlaveConnectionFactory.getInstance().getConnection();
			}
		} catch (SQLException e1) {
			// TODO Auto-generated catch block
			log.error("移除操作连接异常:",e1);
		}
		try {
			if (obj != null) {
				Field[] fiels = obj.getClass().getDeclaredFields();// 获得反射对象集合
				boolean t = obj.getClass().isAnnotationPresent(Table.class);// 获得类是否有注解
				if (t) {
					Table tab = obj.getClass().getAnnotation(Table.class);
						tbl_name =  tab.name();
					sql += tbl_name;
				}
				for (Field fl : fiels) {// 循环组装
					fl.setAccessible(true);// 开启支私有变量的访问权限
					Object tobj = fl.get(obj);
					flag = fl.isAnnotationPresent(Key.class);
					if (flag)
						isKey = fl.getAnnotation(Key.class);
					if (tobj != null && isKey != null) {
						if (!flag_is && isKey.isPrimary()) {
							flag_is = true;
							String tp=fl.getType().getName().toUpperCase();
							if(tp.endsWith("INT")||tp.endsWith("INTEGER")||tp.endsWith("LONG")){
								sql += "  WHERE  " + fl.getName() + "=" + tobj.toString() + "  ";
							}else{
								sql += "  WHERE  " + fl.getName() + "='" + tobj.toString() + "'  ";
							}
							break;
						}
					}
				}
				SqlCommonDAO helper = new SqlCommonDAO();
				try {
					Command c = new Command();
					helper.setConnection(connection);
					helper.setCommand(c);
					c.setCache(false);
					c.setTables(new String[] { tbl_name });
					c.setSql(sql.toString());
					i = helper.executeUpdate();
				} catch (SQLException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				} finally {
					helper.closeConnection(connection);
				}
			}
		} catch (IllegalArgumentException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (IllegalAccessException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		return i > 0 ? true : false;
	}

	/**
	 * 通过ID删除实体对象
	 * 
	 * @param id
	 * @param o
	 * @return
	 */
	@SuppressWarnings("static-access")
	public boolean delete(Integer id, Class<?> o) {
		String tbl_name = "";
		boolean flag = false, flag_is = false;
		int i = 0;
		Key isKey = null;
		String sql = "DELETE FROM ";

		try {
			if (connection == null || connection.isClosed()) {
				connection = SlaveConnectionFactory.getInstance().getConnection();
			}
		} catch (SQLException e1) {
			// TODO Auto-generated catch block
			e1.printStackTrace();
		}

		try {
			if (o != null) {
				Field[] fiels = o.newInstance().getClass().getDeclaredFields();// 获得反射对象集合
				boolean t = o.newInstance().getClass().isAnnotationPresent(Table.class);
				// 获得类是否有注解
				if (t) {
					Table tab = o.newInstance().getClass().getAnnotation(Table.class);
						sql +=tab.name();
				}

				for (Field fl : fiels) {// 循环组装
					fl.setAccessible(true);// 开启支私有变量的访问权限
					flag = fl.isAnnotationPresent(Key.class);
					if (flag)
						isKey = fl.getAnnotation(Key.class);
					if (isKey != null) {
						if (!flag_is && isKey.isPrimary()) {
							flag_is = true;
							String tp=fl.getType().getName().toUpperCase();
							if(tp.endsWith("INT")||tp.endsWith("INTEGER")||tp.endsWith("LONG")){
								sql += "  WHERE  " + fl.getName() + "=" + id + "  ";
							}else{
								sql += "  WHERE  " + fl.getName() + "='" + id + "'  ";
							}
							break;
						}
					}
				}

				SqlCommonDAO helper = new SqlCommonDAO();
				try {
					Command c = new Command();
					helper.setConnection(connection);
					helper.setCommand(c);
					c.setCache(false);
					c.setTables(new String[] { tbl_name });
					c.setSql(sql.toString());
					// System.out.println(sql);
					i = helper.executeUpdate();
				} catch (SQLException e) {
					// TODO Auto-generated catch block
					log.error("删除对象异常:",e);
				} finally {
					helper.closeConnection(connection);
				}
			}
		} catch (InstantiationException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (IllegalAccessException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		return i > 0 ? true : false;
	}

	@SuppressWarnings("static-access")
	public boolean delete(String id, Class<?> o) {
		String tbl_name = "";
		boolean flag = false, flag_is = false;
		int i = 0;
		Key isKey = null;
		String sql = "DELETE FROM ";

		try {
			if (connection == null || connection.isClosed()) {
				connection = SlaveConnectionFactory.getInstance().getConnection();
			}
		} catch (SQLException e1) {
			// TODO Auto-generated catch block
			e1.printStackTrace();
		}

		try {
			if (o != null) {
				Field[] fiels = o.newInstance().getClass().getDeclaredFields();// 获得反射对象集合
				boolean t = o.newInstance().getClass().isAnnotationPresent(Table.class);
				// 获得类是否有注解
				if (t) {
					Table tab = o.newInstance().getClass().getAnnotation(Table.class);
						sql += tab.name();
				}

				for (Field fl : fiels) {// 循环组装
					fl.setAccessible(true);// 开启支私有变量的访问权限
					flag = fl.isAnnotationPresent(Key.class);
					if (flag)
						isKey = fl.getAnnotation(Key.class);
					if (isKey != null) {
						if (!flag_is && isKey.isPrimary()) {
							flag_is = true;
							String tp=fl.getType().getName().toUpperCase();
							if(tp.endsWith("INT")||tp.endsWith("INTEGER")||tp.endsWith("LONG")){
								sql += "  WHERE  " + fl.getName() + "=" + id + "  ";
							}else{
								sql += "  WHERE  " + fl.getName() + "='" + id + "'  ";
							}
							break;
						}
					}
				}

				SqlCommonDAO helper = new SqlCommonDAO();
				try {
					Command c = new Command();
					helper.setConnection(connection);
					helper.setCommand(c);
					c.setCache(false);
					c.setTables(new String[] { tbl_name });
					c.setSql(sql.toString());
					i = helper.executeUpdate();
				} catch (SQLException e) {
					// TODO Auto-generated catch block
					//e.printStackTrace();
					log.error("删除对象异常:",e);
				} finally {
					helper.closeConnection(connection);
				}
			}
		} catch (InstantiationException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (IllegalAccessException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		return i > 0 ? true : false;
	}

/**
 * 获取实体对象
 * @param id
 * @param o
 * @param isCache
 * @return
 */
	@SuppressWarnings("static-access")
	public Object get(Integer id, Class<?> o, boolean isCache) {
		Object obj = null;
		Key isKey = null;
		boolean flag = false, flag_is = false;
		StringBuffer sb = new StringBuffer();
		String n = "";
		try {
			if (connection == null || connection.isClosed()) {
				connection = SlaveConnectionFactory.getInstance().getConnection();
			}
			sb.append("SELECT * FROM ");
			String table = "";
			Field[] fiels = o.newInstance().getClass().getDeclaredFields();// 获得反射对象集合
			boolean tbl = o.newInstance().getClass().isAnnotationPresent(Table.class);// 获得类是否有注解
			if (tbl) {
				Table t = (Table) o.newInstance().getClass().getAnnotation(Table.class);
				table =  t.name();
				n = t.name().substring(t.name().indexOf("_") + 1, t.name().indexOf("_") + 2).toLowerCase();
				sb.append(table).append(" " + n + "  ");
				// sb.append(table);
			}
			for (Field fl : fiels) {// 循环组装
				fl.setAccessible(true);// 开启支私有变量的访问权限
				flag = fl.isAnnotationPresent(Key.class);
				if (flag)
					isKey = fl.getAnnotation(Key.class);
				if (isKey != null) {
					if (!flag_is && isKey.isPrimary()) {
						flag_is = true;
						// sb.append("  WHERE  "+n+"."+fl.getName()+"="+id+"  ");
						sb.append("  WHERE  ").append(n).append(".").append(fl.getName()).append("=").append(id);
						break;
					}
				}
			}
			// System.out.println(sb.toString());
			SqlCommonDAO helper = new SqlCommonDAO();
			try {
				helper.setConnection(connection);
				Command c = new Command();
				helper.setCommand(c);
				c.setCache(isCache);

				c.setTables(new String[] { table });
				c.setSql(sb.toString());
				Result rs = helper.executeQuery();
				if(rs!=null&&rs.getRowCount()>0) obj = JDBCBeanUtil.transTOObject(rs, o);
			} catch (Exception e) {
				// TODO: handle exception
			} finally {
				helper.closeConnection(connection);
			}
		} catch (Exception e) {
			// TODO: handle exception
			//e.fillInStackTrace();
			log.error("根据ID:"+id+"获取对象异常:",e);
		}
		return obj;
	}

	@SuppressWarnings("static-access")
	public Object get(String id, Class<?> o, boolean isCache) {
		Object obj = null;
		Key isKey = null;
		boolean flag = false, flag_is = false;
		StringBuffer sb = new StringBuffer();
		String n = "";
		try {
			if (connection == null || connection.isClosed()) {
				// connection=SlaveConnectionFactory.getConnection();
				connection = SlaveConnectionFactory.getInstance().getConnection();
			}
			sb.append("SELECT * FROM ");
			String table = "";
			Field[] fiels = o.newInstance().getClass().getDeclaredFields();// 获得反射对象集合
			boolean tbl = o.newInstance().getClass().isAnnotationPresent(Table.class);// 获得类是否有注解
			//System.out.println("isTable:" + tbl);
			if (tbl) {
				Table t = (Table) o.newInstance().getClass().getAnnotation(Table.class);
				table = connection.getCatalog() + "." + t.name();
				n = t.name().substring(t.name().indexOf("_") + 1, t.name().indexOf("_") + 2).toLowerCase();
				sb.append(table).append(" " + n + "  ");
				// sb.append(table);
			}
			for (Field fl : fiels) {// 循环组装
				fl.setAccessible(true);// 开启支私有变量的访问权限
				flag = fl.isAnnotationPresent(Key.class);
				if (flag)
					isKey = fl.getAnnotation(Key.class);
				if (isKey != null) {
					if (!flag_is && isKey.isPrimary()) {
						flag_is = true;
						// sb.append("  WHERE  "+n+"."+fl.getName()+"="+id+"  ");
						sb.append("  WHERE  ").append(n).append(".").append(fl.getName()).append("='").append(id + "'");
						break;
					}
				}
			}
			// System.out.println(sb.toString());
			SqlCommonDAO helper = new SqlCommonDAO();
			try {
				helper.setConnection(connection);
				Command c = new Command();
				helper.setCommand(c);
				c.setCache(isCache);
				
				c.setTables(new String[] { table });
				c.setSql(sb.toString());
				Result rs = helper.executeQuery();
				if(rs!=null&&rs.getRowCount()>0) obj = JDBCBeanUtil.transTOObject(rs, o);
			} catch (Exception e) {
				// TODO: handle exception
			} finally {
				helper.closeConnection(connection);
			}
		} catch (Exception e) {
			// TODO: handle exception
			log.error("根据ID:"+id+"获取对象异常:",e);
		}
		return obj;
	}

	/**
	 * 根据一般字段查询记录
	 * 
	 * @param iswhere
	 * @param o
	 * @return
	 */
	@SuppressWarnings("static-access")
	public Object load(Class<?> o, String iswhere, boolean isCache) {
		Object obj = null;
		StringBuffer sb = new StringBuffer();
		// String n="";
		// String table="";
		boolean isTable = false;
		try {
			isTable = o.newInstance().getClass().isAnnotationPresent(Table.class);
		} catch (InstantiationException e1) {
			// TODO 自动生成的 catch 块
			 log.error("bean中没有找到对应的表的注释信息",e1);
			//e1.printStackTrace();
		} catch (IllegalAccessException e1) {
			// TODO 自动生成的 catch 块
		    log.error("bean中没有找到对应的表的注释信息",e1);
			//e1.printStackTrace();
		}// 获得类是否有注解
		try {
			if (connection == null || connection.isClosed()) {
				connection = SlaveConnectionFactory.getInstance().getConnection();
			}
			// Field[]
			// fiels=o.newInstance().getClass().getDeclaredFields();//获得反射对象集合

			//System.out.println("isTable:" + isTable);
			if (isTable) {
				
				Table t = (Table) o.newInstance().getClass().getAnnotation(Table.class);
				// sb.append(connection.getCatalog()).append(".").append(t.name());
				String table = t.name();
				// String table=t.name();
				// System.out.println("table:"+table);
				String n = t.name().substring(t.name().indexOf("_") + 1, t.name().indexOf("_") + 2).toLowerCase();
				
				if (!StringUtil.isNull(iswhere)) {
					if(!iswhere.toUpperCase().contains("SELECT")&&!iswhere.toUpperCase().contains("FROM")) {
						sb.append("SELECT * FROM ");
						sb.append(table).append(" ").append(n);
						sb.append(" ");
						sb.append(iswhere);
					}else {
						sb.append(iswhere);
					}
				}else{
					log.error("参数：[iswhere]; sql条件语句是空的，请检查后使用...");
					return obj;
				}
				// sb.append(table);

				
				SqlCommonDAO helper = new SqlCommonDAO();
				try {
					helper.setConnection(connection);
					Command c = new Command();
					helper.setCommand(c);
					c.setCache(isCache);
					c.setTables(new String[] { table });
					c.setSql(sb.toString());
					Result rs = helper.executeQuery();
					if(rs!=null&&rs.getRowCount()>0)obj = JDBCBeanUtil.transTOObject(rs, o);
				} catch (Exception e) {
					// TODO: handle exception
					log.error("load-sql:" + sb.toString() + "\n",e);
				//	e.fillInStackTrace();
				} finally {
					helper.closeConnection(connection);
				}
			} else {
				log.info("bean中没有找到对应的表的注释信息");
			}
		} catch (Exception e) {
			// TODO: handle exception
			log.error("获得类是否有注解load-sql:" + sb.toString() + "\n",e);
			//e.fillInStackTrace();
		}
		return obj;
	}

	/**
	 * 创建数据库表，删除指定表
	 */

	@SuppressWarnings("static-access")
	public boolean CreateTable(String sql) {
		boolean flag = false;
		Statement state = null;
		try {
			if (connection == null || connection.isClosed()) {
				// connection=SlaveConnectionFactory.getConnection();
				connection = SlaveConnectionFactory.getInstance().getConnection();
			}
			state = connection.createStatement();
			flag = state.execute(sql);
		} catch (SQLException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} finally {
			try {
				if (connection != null) {
					connection.close();
				}
				if (state != null) {
					state.close();
				}
			} catch (Exception e) {
				// TODO: handle exception
				e.printStackTrace();
			}
		}
		return flag;
	}

	/**
	 * oracle 分页查询
	 * 
	 * @param o
	 * @param sql
	 * @return
	 */
	@SuppressWarnings("static-access")
	public List<?> queryByOracle(Class<?> o, int p1, int p2, String desc) {
		List list = null;
		String table = "";
		String isOrder = "";
		SqlCommonDAO helper = new SqlCommonDAO();
		if (!StringUtil.isNull(desc) && "DESC".equals(desc.toUpperCase()))
			isOrder = "ORDER BY ROWNUM DESC";
		StringBuffer sb_sql = new StringBuffer();

		try {
			if (connection == null || connection.isClosed()) {
				// connection=SlaveConnectionFactory.getConnection();
				connection = SlaveConnectionFactory.getInstance().getConnection();
			}
			boolean tbl = o.newInstance().getClass().isAnnotationPresent(Table.class);
			//System.out.println("isTable:" + tbl);
			if (tbl) {
				Table t = (Table) o.newInstance().getClass().getAnnotation(Table.class);
				// table=connection.getCatalog()+"."+t.name();
				table = t.name();
			}

			helper.setConnection(connection);
			Command c = new Command();
			helper.setCommand(c);
			if (SlaveConnectionFactory.EHCACHE) {
				c.setCache(true);
			}
			// if(p1>0&&p2>0){
			sb_sql.append("SELECT * FROM ").append("(").append(" SELECT A.*, ROWNUM RN ").append(" FROM (SELECT * FROM  ").append(table).append("  ").append(isOrder).append(" ) A")
					.append(" WHERE ROWNUM <= ").append(p2).append(" )").append(" WHERE RN >= ").append(p1);
			// }
			c.setTables(new String[] { table });
			c.setSql(sb_sql.toString());
			//System.out.println("sql:" + sb_sql.toString());
			Result rs = helper.executeQuery();
			if(rs!=null&&rs.getRowCount()>0) list = JDBCBeanUtil.transTOList(rs, o);
			//connection.close();
		} catch (SQLException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (IllegalAccessException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (InvocationTargetException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (InstantiationException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} finally {
			helper.closeConnection(connection);
		}
		return list;
	}
	
	/**
	 * 
	 * executeUpdate(增加、修改、删除 操作) (注意事项：无)
	 * 
	 * @param sql
	 * @param obj
	 * @return
	 * @exception
	 * @since 1.0.0
	 */
	@SuppressWarnings("static-access")
	public int executeUpdate(String sql, Object... obj) {
		int k = 0;
		SqlCommonDAO helper = new SqlCommonDAO();
		try {
			if (connection == null || connection.isClosed()) {
				connection = SlaveConnectionFactory.getInstance().getConnection();
			}
			k = helper.executeUpdate(sql,connection, obj);
		} catch (SQLException e) {
			// TODO Auto-generated catch block
			//e.printStackTrace();
			log.error("增加|修改|删除 操作异常：SQL:"+sql,e);
		}
		return k;
	}
	
	/**
	 * 
	 * query(查询方法) (注意事项： – 目前只支持 Map List返回值)
	 * 
	 * @param resultClass
	 *            返回类型 如: Map.class
	 * @return
	 * @throws SQLException
	 * @exception
	 * @since 1.0.0
	 */
	@SuppressWarnings({ "static-access", "unchecked" })
	public <E> E query(String sql, Class<E> maplistClass,String table,boolean isCache ,Object... obj) {
		SqlCommonDAO helper = new SqlCommonDAO();
		try {
			if (connection == null || connection.isClosed()) {
				connection = SlaveConnectionFactory.getInstance().getConnection();
			}
			helper.setConnection(connection);
			Command c = new Command();
			helper.setCommand(c);
			c.setCache(isCache);
			c.setTables(new String[] { table });
			c.setSql(sql);
			
			//System.out.println(sql+"|"+(maplistClass == Map.class));
			ResultSet rs = helper.queryAll(sql, connection, obj);
			//System.out.println(maplistClass+"|"+rs);
			if(rs!=null&&rs.next()) {
				if (maplistClass == Map.class) {
					if (rs.next())
						return (E) getResultMap(rs);
				} else if (maplistClass == List.class) {
					return (E) getResultList(rs);
				} else {
					throw new RuntimeException("" + maplistClass + " 该类型目前还没有做扩展!");
				}
			}
		} catch (SQLException e) {
			log.error("SQL:"+sql, e);
		} finally {
			helper.closeConnection(connection);
		}
		return null;

	}

	

	/*
	 * 解析ResultSet 表列数据
	 */
	private Map<String, Object> getResultMap(ResultSet rs) throws SQLException {
		Map<String, Object> rawMap = new HashMap<String, Object>();
		ResultSetMetaData rsmd = rs.getMetaData(); // 表对象信息
		int count = rsmd.getColumnCount(); // 列数
		// 遍历之前需要调用 next()方法
		if(rs.next()){
			for (int i = 1; i <= count; i++) {
				String key = rsmd.getColumnLabel(i);
				Object value = rs.getObject(key);
				rawMap.put(key, value);
			}
		}
		return rawMap;
	}
	
	/*
	 * 解析ResultSet 表列数据
	 */
	private Map<String, Object> findResultMap(ResultSet rs) throws SQLException {
		Map<String, Object> rawMap = new HashMap<String, Object>();
		ResultSetMetaData rsmd = rs.getMetaData(); // 表对象信息
		int count = rsmd.getColumnCount(); // 列数
		// 遍历之前需要调用 next()方法
		for (int i = 1; i <= count; i++) {
			String key = rsmd.getColumnLabel(i);
			Object value = rs.getObject(key);
			rawMap.put(key, value);
		}
		return rawMap;
	}

	/*
	 * 解析ResultSet 表数据
	 */
	private List<Map<String, Object>> getResultList(ResultSet rs)
			throws SQLException {
		List<Map<String, Object>> rawList = new ArrayList<Map<String, Object>>();
		while (rs.next()) {
			Map<String, Object> rawMap = findResultMap(rs);
			rawList.add(rawMap);
		}
		return rawList;
	}
	
	
	@SuppressWarnings("static-access")
	public  <T> List<T> queryList(Class<T> Clazz,String isWhere,boolean isCache, Object... obj)  throws SQLException {
		String tableName="";
		List<T> list=null;
		String sql="SELECT * FROM  ";
			if (connection == null || connection.isClosed()) {
				connection = ConnectionFactory.getInstance().getConnection();
			}
			
			try {
				boolean	tbl = Clazz.newInstance().getClass().isAnnotationPresent(Table.class);
				if (tbl) {
					Table t = (Table) Clazz.newInstance().getClass().getAnnotation(Table.class);
					tableName =  t.name();
					if(StringUtil.isNull(tableName)){
						log.error("bean映射表名异常:"+tableName);
					}else{
						if(StringUtil.isNull(isWhere)){
							sql=sql+tableName;
						}else{
							sql=sql+tableName+" "+isWhere;
						}
						
					}
				}
			} catch (InstantiationException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			} catch (IllegalAccessException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
			
			SqlCommonDAO helper = new SqlCommonDAO();
			
			helper.setConnection(connection);
			Command c = new Command();
			helper.setCommand(c);
			c.setCache(isCache);
			c.setTables(new String[] { tableName });
			c.setSql(sql);
			ResultSet rs = helper.queryAll(sql, connection, obj);
			if(rs!=null&&rs.next())  
				list= helper.convertToList(rs,Clazz);
		return list;
	}
	
	@SuppressWarnings("static-access")
	public   List<?> queryByList(Class<?> Clazz,String isWhere,boolean isCache, Object... obj)  throws SQLException {
	
		String sql="SELECT * FROM  ";
		String tableName="";
		
		try {

			if (connection == null || connection.isClosed()) {
				connection = ConnectionFactory.getInstance().getConnection();
			}
			
			
			boolean	tbl = Clazz.newInstance().getClass().isAnnotationPresent(Table.class);
			if (tbl) {
				Table t = (Table) Clazz.newInstance().getClass().getAnnotation(Table.class);
				tableName =  t.name();
				if(StringUtil.isNull(tableName)){
					log.error("bean映射表名异常:"+tableName);
				}else{
					if(StringUtil.isNull(isWhere)){
						sql=sql+tableName;
					}else{
						sql=sql+tableName+" "+isWhere;
					}
					
				}
			}
		} catch (InstantiationException e1) {
			// TODO Auto-generated catch block
			e1.printStackTrace();
		} catch (IllegalAccessException e1) {
			// TODO Auto-generated catch block
			e1.printStackTrace();
		}// 获得类是否有注解
		
		SqlCommonDAO helper = new SqlCommonDAO();
			helper.setConnection(connection);
			Command c = new Command();
			helper.setCommand(c);
			c.setCache(isCache);
			c.setTables(new String[] { tableName });
			c.setSql(sql);
			ResultSet rs = helper.queryAll(sql, connection, obj);
			 //List<T> list= helper.convertToList(rs,Clazz);
			List<?> list=null;
			try {
				Result rss = ResultSupport.toResult(rs);
				if(rss!=null&&rss.getRowCount()>0) 	list = JDBCBeanUtil.transTOList(rss, Clazz);
			} catch (IllegalAccessException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			} catch (InvocationTargetException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			} catch (InstantiationException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		return list;
	}
	  /**
     * 对List对象按照某个成员变量进行排序
     * @param list       List对象
     * @param sortField  排序的属性名称
     * @param sortMode   排序方式：ASC，DESC 任选其一
     */
    public  <T> void sortList(List<T> list, final String sortField, final String sortMode) {
        if(list == null || list.size() < 2) {
            return;
        }
        Collections.sort(list, new Comparator<T>() {
           // @Override
            public int compare(T o1, T o2) {
                try {
                    Class clazz = o1.getClass();
                    Field field = clazz.getDeclaredField(sortField); //获取成员变量
                    field.setAccessible(true); //设置成可访问状态
                    String typeName = field.getType().getName().toLowerCase(); //转换成小写

                    Object v1 = field.get(o1); //获取field的值
                    Object v2 = field.get(o2); //获取field的值

                    boolean ASC_order = (sortMode == null || "DESC".equalsIgnoreCase(sortMode));

                    //判断字段数据类型，并比较大小
                    
                    if (typeName.endsWith("int")||typeName.endsWith("integer")) {
                        Integer value1 = Integer.parseInt(v1.toString());
                        Integer value2 = Integer.parseInt(v2.toString());
                        return ASC_order ? value1.compareTo(value2) : value2.compareTo(value1);
                    } else if (typeName.endsWith("double")) {
                        Double value1 = Double.parseDouble(v1.toString());
                        Double value2 = Double.parseDouble(v2.toString());
                        return ASC_order ? value1.compareTo(value2) : value2.compareTo(value1);
                    }else if (typeName.endsWith("long")) {
                        Long value1 = Long.parseLong(v1.toString());
                        Long value2 = Long.parseLong(v2.toString());
                        return ASC_order ? value1.compareTo(value2) : value2.compareTo(value1);
                    } else if (typeName.endsWith("float")) {
                        Float value1 = Float.parseFloat(v1.toString());
                        Float value2 = Float.parseFloat(v2.toString());
                        return ASC_order ? value1.compareTo(value2) : value2.compareTo(value1);
                    } else if (typeName.endsWith("boolean")) {
                        Boolean value1 = Boolean.parseBoolean(v1.toString());
                        Boolean value2 = Boolean.parseBoolean(v2.toString());
                        return ASC_order ? value1.compareTo(value2) : value2.compareTo(value1);
                    } else if (typeName.endsWith("date")) {
                        Date value1 = (Date)(v1);
                        Date value2 = (Date)(v2);
                        return ASC_order ? value1.compareTo(value2) : value2.compareTo(value1);
                    }else if (typeName.endsWith("timestamp")) {
                    	Timestamp value1 = (Timestamp)(v1);
                    	Timestamp value2 = (Timestamp)(v2);
                        return ASC_order ? value1.compareTo(value2) : value2.compareTo(value1);
                    }else if (typeName.endsWith("string")) {
                    	String value1 = v1.toString();
                    	String value2 = v2.toString();
                        return ASC_order ? value1.compareTo(value2) : value2.compareTo(value1);
                    }else if (typeName.endsWith("short")) {
                    	Short value1 = Short.parseShort(v1.toString());
                    	Short value2 = Short.parseShort(v2.toString());
                        return ASC_order ? value1.compareTo(value2) : value2.compareTo(value1);
                    }else if (typeName.endsWith("byte")) {
                    	Byte value1 = Byte.parseByte(v1.toString());
                    	Byte value2 = Byte.parseByte(v2.toString());
                        return ASC_order ? value1.compareTo(value2) : value2.compareTo(value1);
                    }else if (typeName.endsWith("char")) {
                    	Integer value1 = (int)(v1.toString().charAt(0));
                    	Integer value2 = (int)(v2.toString().charAt(0));
                        return ASC_order ? value1.compareTo(value2) : value2.compareTo(value1);
                    }else {
                        //调用对象的compareTo()方法比较大小
                    	//System.out.println("ASC_order:"+ASC_order);
                        Method method = field.getType().getDeclaredMethod("compareTo", new Class[]{field.getType()});
                        method.setAccessible(true); //设置可访问权限
                        int result = (Integer) method.invoke(v1, new Object[]{v2});
                        return ASC_order ? result : result * (-1);
                    }
                } catch (Exception e) {
                	log.error("对List对象按照某个成员变量进行排序异常：",e);
                }

                return 0; //未知类型，无法比较大小
            }
        });
    }

	
    @SuppressWarnings("static-access")
	public   List<?> queryForList(Class<?> Clazz,String sql, Object... obj)  throws SQLException {
    	List<?> list=null;
    	   if (connection == null || connection.isClosed()) {
				connection = SlaveConnectionFactory.getInstance().getConnection();
			}
			ResultSet rs =queryAll(sql, connection, obj);
			try {
				Result rss = ResultSupport.toResult(rs);
				if(rss!=null&&rss.getRowCount()>0) 	list = JDBCBeanUtil.transTOList(rss, Clazz);
			} catch (IllegalAccessException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			} catch (InvocationTargetException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			} catch (InstantiationException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		return list;
	}

    public ResultSet queryAll(String sql, Connection conn, Object... obj) {
		// TODO Auto-generated method stub
		PreparedStatement psts = null;
		ResultSet rs = null;
		try {
			psts = conn.prepareStatement(sql);
			if (obj != null && obj.length > 0) {
				for (int j = 0; j < obj.length; j++) {
					psts.setObject((j + 1), obj[j]);
				}
			}
			rs = psts.executeQuery();
		} catch (SQLException e) {
			log.error("查询-操作异常:",e);
		}
		return rs;
	}
    
    /**
     * 获取所有表名
     * @param dbname
     * @param user
     * @param conn
     * @return
     * @throws UnsupportedEncodingException 
     */
    @SuppressWarnings("static-access")
	public List<String> queryAllTableNames() {
    	List<String> tables=new ArrayList<String>();
    	PreparedStatement psts = null;
    	ResultSet rs = null;
    	try {
			if (connection == null || connection.isClosed()) {
				connection = SlaveConnectionFactory.getInstance().getConnection();
			}
		} catch (SQLException e1) {
			log.error("数据库连接异常:"+e1.getMessage());
		}
		
		try {
			psts = connection.prepareStatement("show tables");
			rs = psts.executeQuery();
			while(rs.next()){
				tables.add(rs.getString(1));
			}
		} catch (SQLException e) {
			log.error("获取数据库所有表名异常:"+e.getMessage());
		} finally {
			try {
				connection.close();
			} catch (SQLException e) {
				log.error("关闭connection连接异常:"+e.getMessage());
			}
		}
		return tables;
	}
    
    public void createBeanByTable(List<String> tables) {
    	PreparedStatement psts = null;
    	ResultSet rs = null;
    	try {
	    	for(String t_name:tables){
				System.out.println(t_name);
					psts = connection.prepareStatement("select * from "+t_name);
					rs = psts.executeQuery();
					if(rs!=null&&rs.next()){
						
					}
			}
    	} catch (SQLException e) {
    		log.error("获取数据库所有表名异常:"+e.getMessage());
    	}finally {
			try {
				connection.close();
			} catch (SQLException e) {
				log.error("关闭connection连接异常:"+e.getMessage());
			}
		}
    }
    
    
    
    
    /**
     * 动态bean使用map<k,n>来代替
     * @param sql
     * @param isCache
     * @return
     */
	@SuppressWarnings("static-access")
	public Map<String,Object> queryMap(String sql) {
		Map<String,Object> bean=null;
		ResultSet rs =null;
			try {
				if (connection == null || connection.isClosed()) {
					connection = ConnectionFactory.getInstance().getConnection();
				  }
				
				  rs =executeQuery(sql);
			} catch (SQLException e) {
				log.error("数据库连接异常:"+e.getMessage());
			}
		 
			try {
				 if(null!=rs){
					 bean=getResultMap(rs);
				 }
			} catch (SQLException e) {
				log.error("getResultMap异常:"+e.getMessage());
			}
		
		return bean;
	}
	
    /**
     * 动态bean使用map<k,n>来代替,返回list<bean>由list<map<k,o>>代替
     * @param sql
     * @param isCache
     * @return
     */
	@SuppressWarnings("static-access")
	public List<Map<String,Object>> queryListMap(String sql) {
		List<Map<String,Object>> list=null;
		ResultSet rs =null;
			try {
				if (connection == null || connection.isClosed()) {
					connection = ConnectionFactory.getInstance().getConnection();
				  }
				
				  rs =executeQuery(sql);
			} catch (SQLException e) {
				log.error("数据库连接异常:"+e.getMessage());
			}
		 
		 
		  try {
			  if(null!=rs){
				  list=getResultList(rs);
			  }
			} catch (SQLException e) {
				log.error("ResultSet是空的异常:"+e.getMessage());
			}		
		return list;
	}
	
	//////////////////////////////////////////////////////////////////////
	private boolean deleteOfTransaction(String id, Class<?> o,SqlCommonDAO helper) {
		String tbl_name = "";
		boolean flag = false, flag_is = false;
		int i = 0;
		Key isKey = null;
		String sql = "DELETE FROM ";


		try {
			if (o != null) {
				Field[] fiels = o.newInstance().getClass().getDeclaredFields();// 获得反射对象集合
				boolean t = o.newInstance().getClass().isAnnotationPresent(Table.class);
				// 获得类是否有注解
				if (t) {
					Table tab = o.newInstance().getClass().getAnnotation(Table.class);
						sql += tab.name();
				}

				for (Field fl : fiels) {// 循环组装
					fl.setAccessible(true);// 开启支私有变量的访问权限
					flag = fl.isAnnotationPresent(Key.class);
					if (flag)
						isKey = fl.getAnnotation(Key.class);
					if (isKey != null) {
						if (!flag_is && isKey.isPrimary()) {
							flag_is = true;
							String tp=fl.getType().getName().toUpperCase();
							if(tp.endsWith("INT")||tp.endsWith("INTEGER")||tp.endsWith("LONG")){
								sql += "  WHERE  " + fl.getName() + "=" + id + "  ";
							}else{
								sql += "  WHERE  " + fl.getName() + "='" + id + "'  ";
							}
							break;
						}
					}
				}

				//SqlCommonDAO helper = new SqlCommonDAO();
				try {
					Command c = new Command();
					//helper.setConnection(connection);
					helper.setCommand(c);
					c.setCache(false);
					c.setTables(new String[] { tbl_name });
					c.setSql(sql.toString());
					i = helper.executeUpdate();
				} catch (Exception e) {
					log.error("删除对象异常:",e);
				} 
			}
		} catch (InstantiationException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (IllegalAccessException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		return i > 0 ? true : false;
	}
	
	
	private boolean deleteOfTransaction(Integer id, Class<?> o,SqlCommonDAO helper) {
		String tbl_name = "";
		boolean flag = false, flag_is = false;
		int i = 0;
		Key isKey = null;
		String sql = "DELETE FROM ";


		try {
			if (o != null) {
				Field[] fiels = o.newInstance().getClass().getDeclaredFields();// 获得反射对象集合
				boolean t = o.newInstance().getClass().isAnnotationPresent(Table.class);
				// 获得类是否有注解
				if (t) {
					Table tab = o.newInstance().getClass().getAnnotation(Table.class);
						sql += tab.name();
				}

				for (Field fl : fiels) {// 循环组装
					fl.setAccessible(true);// 开启支私有变量的访问权限
					flag = fl.isAnnotationPresent(Key.class);
					if (flag)
						isKey = fl.getAnnotation(Key.class);
					if (isKey != null) {
						if (!flag_is && isKey.isPrimary()) {
							flag_is = true;
							String tp=fl.getType().getName().toUpperCase();
							if(tp.endsWith("INT")||tp.endsWith("INTEGER")||tp.endsWith("LONG")){
								sql += "  WHERE  " + fl.getName() + "=" + id + "  ";
							}else{
								sql += "  WHERE  " + fl.getName() + "='" + id + "'  ";
							}
							break;
						}
					}
				}

				//SqlCommonDAO helper = new SqlCommonDAO();
				try {
					Command c = new Command();
					//helper.setConnection(connection);
					helper.setCommand(c);
					c.setCache(false);
					c.setTables(new String[] { tbl_name });
					c.setSql(sql.toString());
					i = helper.executeUpdate();
				} catch (Exception e) {
					log.error("删除对象异常:",e);
				} 
			}
		} catch (InstantiationException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (IllegalAccessException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		return i > 0 ? true : false;
	}
	
	/**
	 * 事物操作，删除第1个对象成功后，在删除第2个对象
	 * @param id
	 * @param o
	 * @param sid
	 * @param so
	 * @return
	 */
	@SuppressWarnings("static-access")
	public boolean deleteByTransaction(String id, Class<?> o,String sid, Class<?> so) {
		int i = 0;
		boolean k1=false,k2=false;
		try {
			if (connection == null || connection.isClosed()) {
				connection = ConnectionFactory.getInstance().getConnection();
			}
			connection.setAutoCommit(false);//将数据库事务自动提交设置为false
			SqlCommonDAO helper = new SqlCommonDAO();
			helper.setConnection(connection);
			helper.setTransaction(false);//通知每个连接操作后不要立即关闭
			       k1=this.deleteOfTransaction( id,  o,helper);
			if(k1) k2=this.deleteOfTransaction(sid, so,helper);
			if(k1&&k2){
				connection.commit();//无异常则提交  
				helper.closeConnection(connection);
				i=1;
			}else{
				//有异常的话进行事务的回滚，把即使执行成功的也进行取消
				connection.rollback();
			}
			
		} catch (SQLException e1) {
			log.error("多删除事物操作异常："+e1.getMessage());
			try {  
				connection.rollback();//有异常的话进行事务的回滚，把即使执行成功的也进行取消  
                log.info("==========异常回滚成功=========");
            } catch (Exception e2) {  
                e2.printStackTrace();  
            }  
		}finally {  
            try {  
                if (connection != null)  
                	connection.close();  //关闭连接  
            } catch (Exception e3) {  
                e3.printStackTrace();  
            }  
        } 

		
		return i > 0 ? true : false;
	}
	
	/**
	 * 事物操作，删除第1个对象成功后，在删除第2个对象
	 * @param id
	 * @param o
	 * @param sid
	 * @param so
	 * @return
	 */
	@SuppressWarnings("static-access")
	public boolean deleteByTransaction(String id, Class<?> o,Integer sid, Class<?> so) {
		int i = 0;
		boolean k1=false,k2=false;
		try {
			if (connection == null || connection.isClosed()) {
				connection = ConnectionFactory.getInstance().getConnection();
			}
			connection.setAutoCommit(false);//将数据库事务自动提交设置为false
			SqlCommonDAO helper = new SqlCommonDAO();
			helper.setConnection(connection);
			helper.setTransaction(false);//通知每个连接操作后不要立即关闭
			       k1=this.deleteOfTransaction( id,  o,helper);
			if(k1) k2=this.deleteOfTransaction(sid, so,helper);
			if(k1&&k2){
				connection.commit();//无异常则提交  
				helper.closeConnection(connection);
				i=1;
			}else{
				//有异常的话进行事务的回滚，把即使执行成功的也进行取消
				connection.rollback();
			}
			
		} catch (SQLException e1) {
			log.error("多删除事物操作异常："+e1.getMessage());
			try {  
				connection.rollback();//有异常的话进行事务的回滚，把即使执行成功的也进行取消  
                log.info("==========异常回滚成功=========");
            } catch (Exception e2) {  
                e2.printStackTrace();  
            }  
		}finally {  
            try {  
                if (connection != null)  
                	connection.close();  //关闭连接  
            } catch (Exception e3) {  
                e3.printStackTrace();  
            }  
        } 

		
		return i > 0 ? true : false;
	}
	
	/**
	 * 事物操作，删除第1个对象成功后，在删除第2个对象
	 * @param id
	 * @param o
	 * @param sid
	 * @param so
	 * @return
	 */
	@SuppressWarnings("static-access")
	public boolean deleteByTransaction(Integer id, Class<?> o,String sid, Class<?> so) {
		int i = 0;
		boolean k1=false,k2=false;
		try {
			if (connection == null || connection.isClosed()) {
				connection = ConnectionFactory.getInstance().getConnection();
			}
			connection.setAutoCommit(false);//将数据库事务自动提交设置为false
			SqlCommonDAO helper = new SqlCommonDAO();
			helper.setConnection(connection);
			helper.setTransaction(false);//通知每个连接操作后不要立即关闭
			       k1=this.deleteOfTransaction( id,  o,helper);
			if(k1) k2=this.deleteOfTransaction(sid, so,helper);
			if(k1&&k2){
				connection.commit();//无异常则提交  
				helper.closeConnection(connection);
				i=1;
			}else{
				//有异常的话进行事务的回滚，把即使执行成功的也进行取消
				connection.rollback();
			}
			
		} catch (SQLException e1) {
			log.error("多删除事物操作异常："+e1.getMessage());
			try {  
				connection.rollback();//有异常的话进行事务的回滚，把即使执行成功的也进行取消  
                log.info("==========异常回滚成功=========");
            } catch (Exception e2) {  
                e2.printStackTrace();  
            }  
		}finally {  
            try {  
                if (connection != null)  
                	connection.close();  //关闭连接  
            } catch (Exception e3) {  
                e3.printStackTrace();  
            }  
        } 

		return i > 0 ? true : false;
	}
	
	/**
	 * 事物操作，删除第1个对象成功后，在删除第2个对象
	 * @param id
	 * @param o
	 * @param sid
	 * @param so
	 * @return
	 */
	@SuppressWarnings("static-access")
	public boolean deleteByTransaction(Integer id, Class<?> o,Integer sid, Class<?> so) {
		int i = 0;
		boolean k1=false,k2=false;
		try {
			if (connection == null || connection.isClosed()) {
				connection = ConnectionFactory.getInstance().getConnection();
			}
			connection.setAutoCommit(false);//将数据库事务自动提交设置为false
			SqlCommonDAO helper = new SqlCommonDAO();
			helper.setConnection(connection);
			helper.setTransaction(false);//通知每个连接操作后不要立即关闭
			       k1=this.deleteOfTransaction( id,  o,helper);
			if(k1) k2=this.deleteOfTransaction(sid, so,helper);
			if(k1&&k2){
				connection.commit();//无异常则提交  
				helper.closeConnection(connection);
				i=1;
			}else{
				//有异常的话进行事务的回滚，把即使执行成功的也进行取消
				connection.rollback();
			}
			
		} catch (SQLException e1) {
			log.error("多删除事物操作异常："+e1.getMessage());
			try {  
				connection.rollback();//有异常的话进行事务的回滚，把即使执行成功的也进行取消  
                log.info("==========异常回滚成功=========");
            } catch (Exception e2) {  
                e2.printStackTrace();  
            }  
		}finally {  
            try {  
                if (connection != null)  
                	connection.close();  //关闭连接  
            } catch (Exception e3) {  
                e3.printStackTrace();  
            }  
        } 

		return i > 0 ? true : false;
	}
	
	/**
	 * 多表记录删除，手动提交事务
	 * 
	 * @param obj
	 * @return
	 */
	@SuppressWarnings("static-access")
	public boolean removeByTransaction(Object[] obj) {
		int i = 0;
		SqlCommonDAO helper = new SqlCommonDAO();
		Command cmd = new Command();
		//boolean String_flag=false,Integer_flag=false,Long_flag=false,Class_flag=false;
		try {
			if (connection == null || connection.isClosed()) {
				connection = ConnectionFactory.getInstance().getConnection();
			}
			connection.setAutoCommit(false);
			helper.setConnection(connection);
			helper.setTransaction(false);
		} catch (SQLException e1) {
			log.error("数据库连接异常:" + e1);
		}

		try {
			/*if(obj instanceof String[]){
				String_flag=true;
			}
			if(obj instanceof Integer[]){
				Integer_flag=true;
			}
			if(obj instanceof Long[]){
				Long_flag=true;
			}
			if(obj instanceof Class<?>[]){
				Class_flag=true;
			}*/
			
			
			  for (int index = 0; index < obj.length; index++) {
				String tbl_name = "";
				Key isKey = null;
				boolean flag = false;
				boolean flag_is = false;
				StringBuffer sb_index = new StringBuffer();

				Field[] fields = obj[index].getClass().getDeclaredFields();
				boolean tbl = obj[index].getClass().isAnnotationPresent(Table.class);
				sb_index.append("DELETE FROM ");
				//System.out.println("isTable:" + tbl);
				if (tbl) {
					Table t = (Table) obj[index].getClass().getAnnotation(Table.class);
					tbl_name = t.name();
					sb_index.append(tbl_name);
				}

				for (Field f : fields) {
					f.setAccessible(true);// 开启私有变量的访问权限
					Object tobj = f.get(obj[index]);
					flag = f.isAnnotationPresent(Key.class);
					if (flag)
						isKey = f.getAnnotation(Key.class);
					if (tobj != null && isKey != null) {
						if (!flag_is && isKey.isPrimary()) {
							flag_is = true;
							String tp=f.getType().getName().toUpperCase();
							if(tp.endsWith("INT")||tp.endsWith("INTEGER")||tp.endsWith("LONG")){
								sb_index.append("  WHERE  " + f.getName() + "=" + tobj.toString());
							}else{
								sb_index.append("  WHERE  " + f.getName() + "='" + tobj.toString()+"'");
							}
							break;
						}
					}
				}// end

				if (sb_index.length()>20) {

					try {
						cmd.setTables(new String[] { tbl_name });
						cmd.setSql(sb_index.toString());
						cmd.setCache(false);

						helper.setCommand(cmd);
						int num = helper.executeCommit();
						i = i + num;
					} catch (Exception e) {
						// TODO Auto-generated catch block
						log.error("保存数据异常：" + e);
						try {
							connection.rollback();
							log.error("-----------数据-回滚异常1-----------");
						} catch (SQLException e1) {
							// TODO Auto-generated catch block
							log.error("数据回滚异常：" + e1);
						}

					}

				}

			  }// for end
			
			
			
			// System.out.println("====i="+i);
			if (i == obj.length) {
				try {
					connection.commit();
					return true;
				} catch (SQLException e) {
					// TODO Auto-generated catch block
					try {
						connection.rollback();
						log.error("-----------数据-回滚异常2-----------");
					} catch (SQLException e1) {
						// TODO Auto-generated catch block
						log.error("数据-回滚异常1：" + e1);
					}
					log.error("手动提交数据异常：" + e);
				}
			} else {
				try {
					connection.rollback();
					log.error("-----------数据-回滚异常3-----------");
				} catch (SQLException e) {
					// TODO Auto-generated catch block
					log.error("数据-回滚异常2：" + e);
				}
			}

		} catch (IllegalAccessException e) {
			// TODO Auto-generated catch block
			log.error("保存数据库进入异常：" + e);
		} finally {
			helper.closeConnection(connection);
		}

		return false;

	}// end
	
	/**
	 * 多表记录保存，手动提交事务
	 * 
	 * @param obj
	 * @return
	 */
	@SuppressWarnings("static-access")
	public boolean saveByTransaction(Object[] obj) {
		int i = 0;
		SqlCommonDAO helper = new SqlCommonDAO();
		
		StringBuffer sql_log = new StringBuffer();
		try {
			if (connection == null || connection.isClosed()) {
				// connection=ConnectionFactory.getConnection();
				connection = ConnectionFactory.getInstance().getConnection();
			}
			connection.setAutoCommit(false);
			helper.setConnection(connection);
			helper.setTransaction(false);
		} catch (SQLException e1) {
			// TODO Auto-generated catch block
			log.error("数据库连接异常:" + e1);
		}

		try {

			for (int index = 0; index < obj.length; index++) {
				String tbl_name = "";
				String name = "";// 记录字段名
				String value = "";// 记录值名称
				String name_auto = "";// 记录字段名
				String value_auto = "";// 记录值名称
				Key k = null;
				boolean flag = false;
				boolean flag_isAuto = false;
				StringBuffer sb_index = new StringBuffer();

				Field[] fields = obj[index].getClass().getDeclaredFields();
				boolean tbl = obj[index].getClass().isAnnotationPresent(Table.class);
				sb_index.append("INSERT INTO ");
				//System.out.println("isTable:" + tbl);
				if (tbl) {
					Table t = (Table) obj[index].getClass().getAnnotation(Table.class);
					tbl_name = t.name();
					/*try {
						//tbl_name = connection.getCatalog() + "." + t.name();
					} catch (SQLException e) {
						// TODO Auto-generated catch block
						e.printStackTrace();
					}*/
					sb_index.append(tbl_name);
				}

				for (Field f : fields) {
					f.setAccessible(true);// 开启私有变量的访问权限
					Object v = f.get(obj[index]);
					flag = f.isAnnotationPresent(Key.class);
					if (flag)
						k = f.getAnnotation(Key.class);
					Colum c = f.getAnnotation(Colum.class);
					if (v != null && k != null) {
						if (c != null && f.getName().toUpperCase().equals(c.columName().toUpperCase()))
							name += f.getName() + ",";
						else
							name += c.columName() + ",";
						if (f.getType().getName().equals("java.lang.String"))
							value += "'" + v.toString() + "',";
						else if (f.getType().getName().equals("java.util.Date")) {
							value += "CURDATE(),";
							// System.out.println("f.getType().getName():"+f.getType().getName());
						} else if (f.getType().getName().equals("java.sql.Timestamp")) {
							value += "NOW(),";
							// System.out.println("f.getType().getName():"+f.getType().getName());
						} else
							value += v.toString() + ",";

					} else {
						if (!flag_isAuto && k.isPrimary() && k.isAuto()) {
							name_auto = f.getName() + ",";
							value_auto = "SEQ_" + tbl_name.toUpperCase() + ".NEXTVAL,";
							flag_isAuto = true;
						}
					}

				}// end

				if (name.length() > 0 && value.length() > 0) {
					name = name.substring(0, name.length() - 1);
					value = value.substring(0, value.length() - 1);

					// 判断方言是否为mysql，不用关心自增序列
					// 判断方言Oracle，必须加上自增序列
					if (ConnectionFactory.getDialect().toLowerCase().equals("mysqldialect")) {
						sb_index.append("(").append(name).append(")").append("  VALUES").append("(").append(value).append(")");
					} else if (ConnectionFactory.getDialect().toLowerCase().equals("oracledialect")) {
						sb_index.append("(").append(name_auto + name).append(")").append("  VALUES").append("(").append(value_auto + value).append(")");
					}

					try {
						Command cmd = new Command();
						cmd.setTables(new String[] { tbl_name });
						cmd.setSql(sb_index.toString());
						cmd.setCache(false);

						helper.setCommand(cmd);
						int num = helper.executeCommit();
						sql_log.append("\n"+sb_index.toString());
						i = i + num;
					} catch (Exception e) {
						// TODO Auto-generated catch block
						log.error("保存数据异常：" + e);
						try {
							connection.rollback();
							log.error("-----------数据-回滚异常1-----------");
						} catch (SQLException e1) {
							// TODO Auto-generated catch block
							log.error("数据回滚异常：" + e1);
						}

					}

				}

			}// for end

			// System.out.println("====i="+i);
			if (i == obj.length) {
				try {
					connection.commit();
					log.info("多事物保存执行记录："+sql_log.toString());
					return true;
				} catch (SQLException e) {
					// TODO Auto-generated catch block
					try {
						connection.rollback();
						log.error("-----------数据-回滚异常2-----------");
					} catch (SQLException e1) {
						// TODO Auto-generated catch block
						log.error("数据-回滚异常1：" + e1);
					}
					log.error("手动提交数据异常：" + e);
				}
			} else {
				try {
					connection.rollback();
					log.error("-----------数据-回滚异常3-----------");
				} catch (SQLException e) {
					// TODO Auto-generated catch block
					log.error("数据-回滚异常2：" + e);
				}
			}

		} catch (IllegalAccessException e) {
			// TODO Auto-generated catch block
			log.error("保存数据库进入异常：" + e);
		} finally {
			sql_log.setLength(0);
			helper.closeConnection(connection);
		}

		return false;

	}// end

	

	/**
	 * 更新实体对象,手动开启事物
	 * 
	 * @param obj
	 * @return
	 * @throws IllegalArgumentException
	 * @throws IllegalAccessException
	 * @throws MyException
	 */
	@SuppressWarnings({ "static-access"})
	public  boolean updateByTransaction(Object[] obj) {
		StringBuffer sql_log = new StringBuffer();
		int i = 0;
		SqlCommonDAO helper = new SqlCommonDAO();
		try {
			try {
				if (connection == null || connection.isClosed()) {
					connection = ConnectionFactory.getInstance().getConnection();
				}
				connection.setAutoCommit(false);
				helper.setConnection(connection);
				helper.setTransaction(false);
			} catch (SQLException e1) {
				// TODO Auto-generated catch block
				e1.printStackTrace();
			}
			for (int index = 0; index < obj.length; index++) {
				String wh = "";// 记录字段名
				String k = "";
				boolean bl = false;// 记录主键是否为空
				boolean flag = false;
				Key isKey = null;
				String tbl_name = "";
				StringBuffer sb_index = new StringBuffer();
				sb_index.append("UPDATE ");
				
				Field[] fiels = obj[index].getClass().getDeclaredFields();// 获得反射对象集合
				boolean t = obj[index].getClass().isAnnotationPresent(Table.class);// 获得类是否有注解
				//System.out.println("isTable:" + t);
				if (t) {
					Table tab = obj[index].getClass().getAnnotation(Table.class);
						tbl_name =  tab.name();
						sb_index.append(tbl_name+"  SET ");
					for (Field fl : fiels) {// 循环组装
						fl.setAccessible(true);// 开启支私有变量的访问权限
						Object tobj = fl.get(obj[index]);
						Colum c = fl.getAnnotation(Colum.class);
						flag = fl.isAnnotationPresent(Key.class);
						if (flag)
							isKey = fl.getAnnotation(Key.class);
						String tp=fl.getType().getName().toLowerCase();
						if (tobj != null && isKey != null) {
							if (!bl && isKey.isPrimary()) {// 判断是否存在主键
								bl = true;
								if (tp.endsWith("string"))
									k = "  WHERE  " + fl.getName() + "='" + tobj.toString() + "'  ";
								else
									k = "  WHERE  " + fl.getName() + "=" + tobj.toString() + "  ";

							} 
							else {
								if(c != null &&tp.endsWith("date")){
									if(tobj!=null){
										wh += fl.getName() + "=CURDATE(),";
									}
								}else if(c != null &&tp.endsWith("timestamp")){
									if(tobj!=null){
										wh += fl.getName() + "=NOW(),";
									 }
								}else if (c != null && tp.equals(c.columName().toLowerCase())) {
									if (tp.endsWith("string"))
										wh += fl.getName() + "='" + tobj.toString() + "',";
									else
										wh += fl.getName() + "=" + tobj.toString() + ",";
								} else {
									if (tp.endsWith("string"))
										wh += c.columName() + "='" + tobj.toString() + "',";
									else
										wh += c.columName() + "=" + tobj.toString() + ",";
								}
							}
						}
					}
					if (wh.length() > 0 && k.length() > 0) {
						
						wh = wh.substring(0, wh.lastIndexOf(","));
						k = k.substring(0, k.length() - 1);
						sb_index.append(wh + k);
						//System.out.println(sql);
						//SqlCommonDAO helper = new SqlCommonDAO();
						try {
							//helper.setConnection(connection);
							Command c = new Command();
							helper.setCommand(c);
							c.setCache(false);
							c.setTables(new String[] { tbl_name });
							c.setSql(sb_index.toString());
							int num = helper.executeUpdate();
							sql_log.append("\n"+sb_index.toString());
							i = i + num;
						} catch (Exception e) {
							// TODO Auto-generated catch block
							log.error("删除操作异常sql:"+sb_index.toString(),e);
							try {
								connection.rollback();
								log.error("-----------数据-回滚异常1-----------");
							} catch (SQLException e1) {
								log.error("数据回滚异常：" + e1);
							}
						}
					}
					// log.info("\n"+sql);
				}
			}
			
			if (i == obj.length) {
				try {
					connection.commit();
					log.info("多事物更新执行记录："+sql_log.toString());
					return true;
				} catch (SQLException e) {
					// TODO Auto-generated catch block
					try {
						connection.rollback();
						log.error("-----------数据-回滚异常2-----------");
					} catch (SQLException e1) {
						// TODO Auto-generated catch block
						log.error("数据-回滚异常22：" + e1);
					}
					log.error("手动提交数据异常：" + e);
				}
			} else {
				try {
					connection.rollback();
					log.error("-----------数据-回滚异常3-----------");
				} catch (SQLException e) {
					// TODO Auto-generated catch block
					log.error("数据-回滚异常33：" + e);
				}
			}
			
		}catch (IllegalAccessException e) {
			// TODO Auto-generated catch block
			log.error("保存数据库进入异常：" + e);
		} finally {
			sql_log.setLength(0);
			helper.closeConnection(connection);
		}

		return i > 0 ? true : false;
	}

	
	////////////////////////////////////////////////////////////////
	
}
