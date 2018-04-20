package com.easyframework.core.cache;

public class Key {

	private String key;
	private String sql;
	private Object[] params;
	private String[] tables;
	private String pakage_clazz;//缓存类名
	
	public Key(String key,String sql,String[] tables,Object[] params)
	{
	  this.key=key;
	  this.tables=tables;
	  this.sql=sql;
	  this.params=params;
	}
	
	public Key(String pakage_clazz,String key,String sql,String[] tables,Object[] params)
	{
	  this.key=key;
	  this.tables=tables;
	  this.sql=sql;
	  this.params=params;
	  this.pakage_clazz=pakage_clazz;
	}
	
	public String getKey() {
	  return key;
	}
	public void setKey(String key) {
	  this.key = key;
	}
	public String[] getTables() {
	  return tables;
	}
	public void setTables(String[] tables) {
	  this.tables = tables;
	}
	public String getSql() {
	  return sql;
	}
	public void setSql(String sql) {
	  this.sql = sql;
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
