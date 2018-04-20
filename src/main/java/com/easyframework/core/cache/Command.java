package com.easyframework.core.cache;

import java.io.Serializable;

public class Command implements Serializable {

	/**
	 * 
	 */
	private static final long serialVersionUID = 1L;

	private String sql;//sql语句
	private String[] tables;//表当执行增删改的时候，就只放需要增删改的表，查询时放查询所需的表
	private Object[] params;//参数
	private boolean isCache=false;//是否缓存,默认是不缓存
	private String pakage_clazz;//缓存类名
	
	public boolean isCache() {
	  return isCache;
	}
	
	public void setCache(boolean isCache) {
	  this.isCache = isCache;
	}
	
	public Command()
	{
	  
	}
	public Command(String sql,String[] tables,Object[] params,boolean isCache)
	{
	  this.sql=sql;
	  this.tables=tables;
	  this.params=params;
	  this.isCache=isCache;
	}
	public Command(String pakage_clazz,String sql,String[] tables,Object[] params,boolean isCache)
	{
	  this.sql=sql;
	  this.tables=tables;
	  this.params=params;
	  this.isCache=isCache;
	  this.pakage_clazz=pakage_clazz;
	}
	public String getSql() {
	  return sql;
	}
	public void setSql(String sql) {
	  this.sql = sql;
	}
	public String[] getTables() {
	  return tables;
	}
	public void setTables(String[] tables) {
	  this.tables = tables;
	}
	public Object[] getParams() {
	  return params;
	}
	public void setParams(Object[] params) {
	  this.params = params;
	}

	public String getPakage_clazz() {
		return pakage_clazz;
	}

	public void setPakage_clazz(String pakage_clazz) {
		this.pakage_clazz = pakage_clazz;
	}
	
	
}
