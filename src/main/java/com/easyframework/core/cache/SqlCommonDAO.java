package com.easyframework.core.cache;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import javax.servlet.jsp.jstl.sql.Result;
import javax.servlet.jsp.jstl.sql.ResultSupport;

import net.sf.ehcache.Cache;
import net.sf.json.JSONArray;

import org.apache.log4j.Logger;

import com.easyframework.core.db.EncryptUtils;
import com.easyframework.core.db.PropertiesUtils;


public class SqlCommonDAO {
	private static final Logger log = Logger.getLogger(SqlCommonDAO.class);
	
	private Connection connection;
	private Command command;// 提交的命令
	private static CacheProvider provider;// 缓存管理器
	
	private boolean isTransaction=true;//事物默认自动提交
	
	@SuppressWarnings("rawtypes")
	private static List keys = new ArrayList();// 保存key

	
	
	
	
	public boolean isTransaction() {
		return isTransaction;
	}

	public void setTransaction(boolean isTransaction) {
		this.isTransaction = isTransaction;
	}

	public SqlCommonDAO() {
		//super();
		// TODO Auto-generated constructor stub
		/*provider=getCacheProvider();
		if(provider!=null){
			log.info("====================provider======================");
		}*/
	}

	public void setConnection(Connection connection) throws SQLException {
		//log.info("拿到一个连接...");
		this.connection = connection;
	}
	public Command getCommand() {
		return command;
	}

	public void setCommand(Command command) {
		this.command = command;
	}
	
	/**
	 * 执行查询，处理完只要要关闭ResultSet和Connection对象
	 * 
	 * @return ResultSet
	 */
	public Result executeQueryByCache() {
		PreparedStatement pst = null;
		Statement st = null;
		ResultSet resultSet = null;
		Result rs = null;
		String key = null;
		if(EncryptUtils.LOCK){
			if (command.isCache()) {
				// 从缓存中取出结果
				key = getkey(command.getSql(), command.getParams());
				try {
					rs = (Result) getCacheProvider().get(command.getPakage_clazz(),key);
				//	rs = (Result) provider.get(command.getPakage_clazz(),key);
				} catch (Exception e) {
					// TODO Auto-generated catch block
					//e.printStackTrace();
					log.error("从缓存中取出结果异常："+e.getMessage());
				}
			}
			if (rs != null) {
				return rs;
			}
			try {
				if (command.getParams() != null && command.getParams().length > 0) {
					pst = connection.prepareStatement(command.getSql());
					setValues(pst, command.getParams());
					resultSet = pst.executeQuery();
				} else {
					st = connection.createStatement();
					resultSet = st.executeQuery(command.getSql());
	
				}
				
				rs = ResultSupport.toResult(resultSet);
				
				if (command.isCache()) {
					// 放入缓存中
					Map<String,Cache> map=getCacheProvider().getMap();
					Cache cahce=map.get(command.getPakage_clazz());
					getCacheProvider().put(cahce,key, rs);
					register(new Key(command.getPakage_clazz(),key, command.getSql(), command.getTables(),command.getParams()));
				}
				Sql_Format(command.getSql());
			} catch (Exception e) {
				log.error("放入缓存中异常："+e.getMessage());
			} finally {
				if(isTransaction){
				  this.closePreparedStatement(pst);
				  this.close(connection, st, resultSet);
				}
			}
		}
		
		return rs;
	}
	
	public static void Sql_Format(String sql){
		
		String s=sql.toUpperCase()
		  .replaceAll("SELECT","\n SELECT \n\t")
		  .replaceAll(" DELETE ","\n DELETE \n\t")
		  .replaceAll(" UPDATE ","\n UPDATE \n\t")
		  .replaceAll(" SET ","\n SET \n\t")
		  .replaceAll("INSERT","\n INSERT ")
		  .replaceAll("INTO"," INTO \n\t")
		  .replaceAll("VALUES","\n VALUES \n\t")
		  .replaceAll(",",",\n\t")
		  .replaceAll(" FROM ","\n FROM \n\t")
		  .replaceAll(" RIGHT JOIN ","\n RIGHT JOIN \n\t")
		  .replaceAll(" LEFT JOIN ","\n LEFT JOIN \n\t")
		  .replaceAll(" ON ","\n ON \n\t")
		  .replaceAll(" WHERE ","\n WHERE \n\t")
		  .replaceAll(" LIMIT ","\n LIMIT \n\t")
		  .replaceAll(" ORDER BY ","\n ORDER BY \n\t")
		  .replaceAll(" GROUP BY ","\n GROUP BY \n\t");
		

	    if(PropertiesUtils.Develop_Mode&&PropertiesUtils.SQL_FORMAT){
			log.info("\n\n---SQL:-------------------------------------------------------------------\n"
						+s
						+"\n---------------------------------------------------------------------------\n");
	    }
		
		
	}
	/**
	 * 执行查询，处理完只要要关闭ResultSet和Connection对象
	 * 
	 * @return ResultSet
	 */
	public Result executeQuery() {
		PreparedStatement pst = null;
		Statement st = null;
		ResultSet resultSet = null;
		Result rs = null;
		String key = null;
		if(EncryptUtils.LOCK){
			if (command.isCache()) {
				// 从缓存中取出结果
				key = getkey(command.getSql(), command.getParams());
				rs = (Result) getCacheProvider().get(key);
			}
			if (rs != null) {
				return rs;
			}
			try {
				if (command.getParams() != null && command.getParams().length > 0) {
					pst = connection.prepareStatement(command.getSql());
					setValues(pst, command.getParams());
					resultSet = pst.executeQuery();
				} else {
					st = connection.createStatement();
					resultSet = st.executeQuery(command.getSql());
	
				}
				//log.info("command.getSql:"+command.getSql());
				rs = ResultSupport.toResult(resultSet);
				if (command.isCache()) {
					// 放入缓存中
					getCacheProvider().put(key, rs);
					register(new Key(key, command.getSql(), command.getTables(),
							command.getParams()));
				}
				Sql_Format(command.getSql());
			} catch (Exception e) {
				log.error("放入缓存中异常："+e.getMessage());
			} finally {
				if(isTransaction){
				this.closePreparedStatement(pst);
				this.close(connection, st, resultSet);
				}
			}
		}
		return rs;
	}

	/**
	 * 执行添加，修改，删除操作
	 */
	public int executeUpdate() {
		PreparedStatement pst = null;
		Statement st = null;
		int rs =0;
		if(EncryptUtils.LOCK){
			try {
				if (command.getParams() != null && command.getParams().length > 0) {
					pst = connection.prepareStatement(command.getSql());
					setValues(pst, command.getParams());
					rs = pst.executeUpdate();
				} else {
					st = connection.createStatement();
					rs = st.executeUpdate(command.getSql());
				}
				
				if (command.isCache()) {
					// 更新缓存
					update(command.getTables()[0], connection);
				}
				Sql_Format(command.getSql());
					
					
			} catch (Exception e) {
				log.error("出现异常:"+command.getSql(),e);
			} finally {
				if(isTransaction){
				this.closePreparedStatement(pst);
				this.close(connection, st, null);
				}
			}
		}
		return rs;
	}

	
	/**
	 * 手动提交事务多表，执行添加，修改，删除操作
	 */
	public int executeCommit() {
		Statement st = null;
		int rs = 0;
		if(EncryptUtils.LOCK){
			try {
				st = connection.createStatement();
				rs = st.executeUpdate(command.getSql());
				
				Sql_Format(command.getSql());
			} catch (Exception e) {
				log.error("手动提交事务异常："+e.getMessage());
			} 
		}else{
			log.error("LOCK为false操作受限");
		}
		return rs;
	}
	
	
	private void setValues(PreparedStatement pst, Object[] values)
			throws SQLException {
		for (int i = 0; i < values.length; i++) {
			pst.setObject(i + 1, values[i]);
		}
	}

	public static synchronized CacheProvider getCacheProvider() {
		if (provider == null)
			provider = new CacheProviderImp();
		return provider;
	}

	@SuppressWarnings("unchecked")
	private void register(Key key) {
		keys.add(key);
	}

	private String getkey(String sql, Object[] values) {
		StringBuffer buffer = new StringBuffer(sql);
		if (values != null) {
			for (int i = 0; i < values.length; i++) {
				buffer.append(values[i]);
			}
			return buffer.toString();
		}else{
			return sql;
		}
	}

	/**
	 * 更新缓存
	 * 
	 * @param table
	 * @param connection
	 */
	private void update(String table, Connection connection) {
		if(EncryptUtils.LOCK){
			for (int i = 0; i < keys.size(); i++) {
				Key key = (Key) keys.get(i);
				for (int j = 0; j < key.getTables().length; j++) {
					if (key.getTables()[j].equals(table)) {
						PreparedStatement pst = null;
						Statement st = null;
						ResultSet resultSet = null;
						Result rs = null;
						try {
							if (key.getParams() != null
									&& key.getParams().length > 0) {
								pst = connection.prepareStatement(key.getSql());
								setValues(pst, key.getParams());
								resultSet = pst.executeQuery();
							} else {
								st = connection.createStatement();
								resultSet = st.executeQuery(key.getSql());
	
							}
							//System.out.println(key.getSql());
							rs = ResultSupport.toResult(resultSet);
							// 移除旧的对象
							getCacheProvider().remove(key.getKey());
							// 插入新的对象
							getCacheProvider().put(key.getKey(), rs);
							
						} catch (Exception e) {
							log.error("更新缓存异常："+e.getMessage());
						} finally {
							if(isTransaction){
							this.close(connection, st, resultSet);
							}
						}
					}
				}
			}
		}
	}

	/**
	 * 保存记录返回新的主键
	 * @param sql
	 * @param conn
	 * @param obj
	 * @return
	 */
	public String executeUpdateForKey(String sql, Connection conn, Object... obj) {
		// TODO Auto-generated method stub
		int i = 0;
		String newID=null;
		PreparedStatement psts = null;
		if(EncryptUtils.LOCK){
			try {
				psts = conn.prepareStatement(sql,Statement.RETURN_GENERATED_KEYS);
				if (obj != null && obj.length > 0) {
					for (int j = 0; j < obj.length; j++) {
						psts.setObject((j + 1), obj[j]);
					}
				}
				           i=   psts.executeUpdate();
				        //   System.out.println("i:"+i);
				    if(i>0){       
				       ResultSet rs = psts.getGeneratedKeys();
						if(rs.next()){newID=rs.getString(1);}
				    }
				    this.Sql_Format(sql);
			} catch (SQLException e) {
				log.error("增删改操作异常:",e);
			} finally {
				if(isTransaction){
				this.closeConnection(conn);
				this.closePreparedStatement(psts);
				}
			}
		}
		return newID;
	}
	
	public int executeUpdate(String sql, Connection conn, Object... obj) {
		// TODO Auto-generated method stub
		int i = 0;
		PreparedStatement psts = null;
		if(EncryptUtils.LOCK){
			try {
				psts = conn.prepareStatement(sql);
				if (obj != null && obj.length > 0) {
					for (int j = 0; j < obj.length; j++) {
						psts.setObject((j + 1), obj[j]);
					}
				}
				i = psts.executeUpdate();
				this.Sql_Format(sql);
			} catch (SQLException e) {
				log.error("增删改操作异常:",e);
			} finally {
				if(isTransaction){
				this.closeConnection(conn);
				this.closePreparedStatement(psts);
				}
			}
		}
		return i;
	}

	public ResultSet queryAll(String sql, Connection conn, Object... obj) {
		// TODO Auto-generated method stub
		PreparedStatement psts = null;
		ResultSet rs = null;
		if(EncryptUtils.LOCK){
			try {
				psts = conn.prepareStatement(sql);
				if (obj != null && obj.length > 0) {
					for (int j = 0; j < obj.length; j++) {
						psts.setObject((j + 1), obj[j]);
					}
				}
				rs = psts.executeQuery();
				this.Sql_Format(sql);
			} catch (SQLException e) {
				log.error("查询操作异常:"+e.getMessage());
			}
		}
		return rs;
	}

	/**
	 * jdbc查询结果集result转换成对应list集合
	 * @param rs
	 * @param t
	 * @return
	 * @throws SQLException
	 */
	@SuppressWarnings({ "hiding", "unchecked", "deprecation" })
	public  <T> List<T> convertToList(ResultSet rs,Class<T> t)  {
	    List<Map<String, Object>> list = new ArrayList<Map<String, Object>>();
	    List<T> resultList=null;
	    if(EncryptUtils.LOCK){
		    try {
				ResultSetMetaData md = (ResultSetMetaData) rs.getMetaData();
				int columnCount = md.getColumnCount();
				while (rs.next()) {
				  Map<String, Object> rowData = new HashMap<String, Object>();
				  for (int i = 1; i <= columnCount; i++) {
						  rowData.put(md.getColumnName(i), rs.getObject(i));
				  }
				  list.add(rowData);
				}
				
			  if(list!=null&&list.size()>0){
				 // System.out.println(list);
				JSONArray jr = JSONArray.fromObject(list);
				resultList = JSONArray.toList(jr, t);
			  }
			} catch (Exception e) {
				// TODO Auto-generated catch block
				log.error("JSONArray转换异常："+e.getMessage());
			}finally{
				this.closeResultSet(rs);
			}
	    }
	    return resultList;
	  }
	
	
	
	public void closeResultSet(ResultSet set) {
		try {
			if (set != null) {
				set.close();
				//log.info("ResultSet.close");
			}
		} catch (SQLException e) {
			log.error("closeResultSet异常:"+e.getMessage());
		}
	}

	public void closeStatement(Statement statement) {
		try {
			if (statement != null) {
				statement.close();
				//log.info("statement.close");
			}
		} catch (SQLException e) {
			log.error("closeStatement异常:"+e.getMessage());
		}
	}

	public void closePreparedStatement(PreparedStatement ps) {
		try {
			if (ps != null) {
				ps.close();
				//log.info("PreparedStatement.close");
			}
		} catch (SQLException e) {
			log.error("closePreparedStatement异常:"+e.getMessage());
		}
	}

	public void closeConnection(Connection connection) {
		try {
			if (connection != null) {
				connection.close();
				//log.info("连接被销毁...");
			}
		} catch (SQLException e) {
			log.error("closeConnection异常:"+e.getMessage());
		}
	}

	public void close(Connection connection, Statement statement, ResultSet set) {
		closeResultSet(set);
		closeStatement(statement);
		closeConnection(connection);
	}

}
