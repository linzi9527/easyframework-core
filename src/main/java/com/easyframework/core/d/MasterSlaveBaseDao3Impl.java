package com.easyframework.core.d;

import java.sql.SQLException;
import java.util.List;
import java.util.Map;

import com.easyframework.core.dbms.DBMasterSlaveHelper3;

public class MasterSlaveBaseDao3Impl implements IMsaterSlaveBaseDao {
	
	public	DBMasterSlaveHelper3 dbHelper=DBMasterSlaveHelper3.getInstance();

	public boolean save(Object o) throws Exception {
		// TODO Auto-generated method stub
		return dbHelper.save(o);
	}

	public boolean update(Object o) throws Exception {
		// TODO Auto-generated method stub
		return dbHelper.update(o);
	}


	public List<?> queryTables(Class<?> c, String[] link_name,String sql,boolean isCache) throws Exception {
		// TODO Auto-generated method stub
		return dbHelper.queryTables(c, link_name,sql,isCache);
	}


	public boolean remove(Object o) throws Exception {
		// TODO Auto-generated method stub
		return dbHelper.remove(o);
	}


	public boolean delete(Integer id,Class<?> o) throws Exception {
		// TODO Auto-generated method stub
		return dbHelper.delete(id, o);
	}


	public Object get(Integer id, Class<?> o,boolean isCache) throws Exception {
		// TODO Auto-generated method stub
		return dbHelper.get(id, o,isCache);
	}


	public boolean CreateTable(String sql) throws Exception {
		// TODO Auto-generated method stub
		return dbHelper.CreateTable(sql);
	}



	public List<?> query(Class<?> o,boolean isCache) throws Exception {
		// TODO Auto-generated method stub
		return dbHelper.find(o,isCache);
	}

	@SuppressWarnings("static-access")
	public List<?> querys(Class<?> o,boolean isCache) throws Exception {
		// TODO Auto-generated method stub
		return dbHelper.querys(o,isCache);
	}

	public List<?> queryList(Class<?> o, String isWhere, boolean isCache)
			throws Exception {
		// TODO Auto-generated method stub
		return dbHelper.queryList(o, isWhere, isCache);
	}

	public Object load(Class<?> o, String iswhere,boolean isCache) throws Exception {
		// TODO Auto-generated method stub
		return dbHelper.load(o, iswhere, isCache);
	}

	public boolean saveByTransaction(Object[] o) throws Exception {
		// TODO Auto-generated method stub
		return dbHelper.saveByTransaction(o);
	}

	public Object get(String id, Class<?> o, boolean isCache) throws Exception {
		// TODO Auto-generated method stub
		return dbHelper.get(id, o, isCache);
	}
	public boolean remove(String id,Class<?> o) throws Exception {
		// TODO Auto-generated method stub
		return dbHelper.delete(id, o);
	}

	public List<?> query(Class<?> c, String sql, boolean isCache)
			throws Exception {
		// TODO Auto-generated method stub
		return  dbHelper.query(c, sql, isCache);
	}

	public int executeCUD(String sql, Object... obj) {
		// TODO Auto-generated method stub
		return dbHelper.executeUpdate(sql, obj);
	}

	public <E> E query(String sql, Class<E> maplistClass,String table,boolean isCache ,Object... obj) {
		// TODO Auto-generated method stub
		return dbHelper.query(sql, maplistClass,table,isCache, obj);
	}

	public <T> List<T> queryList(Class<T> resultClass,
			String isWhere,boolean isCache, Object... obj) throws SQLException {
		// TODO Auto-generated method stub
		return dbHelper.queryList(resultClass, isWhere, isCache,obj);
	}

	public <T> void sortList(List<T> list, String sortField, String sortMode) {
		// TODO Auto-generated method stub
		dbHelper.sortList(list, sortField, sortMode);
	}

	public List<?> queryByList(Class<?> resultClass,
			String isWhere, boolean isCache, Object... obj) throws SQLException {
		// TODO Auto-generated method stub
		return dbHelper.queryByList(resultClass, isWhere, isCache, obj);
	}

	public List<?> queryForList(Class<?> Clazz, String sql, Object... obj) throws SQLException  {
		// TODO Auto-generated method stub
		return dbHelper.queryForList(Clazz, sql, obj);
	}

	public String saveForKey(Object o) throws Exception {
		// TODO Auto-generated method stub
		return dbHelper.saveForKey(o);
	}

	//@Override
	public boolean saveUpdate(Object o) throws Exception {
		// TODO Auto-generated method stub
		return dbHelper.saveUpdate(o);
	}

	public Map<String, Object> queryMap(String sql) {
		// TODO Auto-generated method stub
		return dbHelper.queryMap(sql);
	}

	public List<Map<String, Object>> queryListMap(String sql) {
		// TODO Auto-generated method stub
		return dbHelper.queryListMap(sql);
	}

	public boolean deleteByTransaction(String id, Class<?> o, String sid,
			Class<?> so) {
		// TODO Auto-generated method stub
		return dbHelper.deleteByTransaction(id, o, sid, so);
	}

	public boolean deleteByTransaction(String id, Class<?> o, Integer sid,
			Class<?> so) {
		// TODO Auto-generated method stub
		return dbHelper.deleteByTransaction(id, o, sid, so);
	}

	public boolean deleteByTransaction(Integer id, Class<?> o, String sid,
			Class<?> so) {
		// TODO Auto-generated method stub
		return dbHelper.deleteByTransaction(id, o, sid, so);
	}

	public boolean deleteByTransaction(Integer id, Class<?> o, Integer sid,
			Class<?> so) {
		// TODO Auto-generated method stub
		return dbHelper.deleteByTransaction(id, o, sid, so);
	}

	public boolean removeByTransaction(Object[] obj) {
		// TODO Auto-generated method stub
		return dbHelper.removeByTransaction(obj);
	}

	public boolean updateByTransaction(Object[] obj) {
		// TODO Auto-generated method stub
		return dbHelper.updateByTransaction(obj);
	}

	public boolean deleteAfterSaveByTransaction(Object delObj, Object saveObj) {
		// TODO Auto-generated method stub
		return dbHelper.deleteAfterSaveByTransaction(delObj, saveObj);
	}

	public boolean deleteAfterUpdateByTransaction(Object delObj,
			Object updateObj) {
		// TODO Auto-generated method stub
		return dbHelper.deleteAfterUpdateByTransaction(delObj, updateObj);
	}

	public boolean saveAfterDeleteByTransaction(Object saveObj, Object delObj) {
		// TODO Auto-generated method stub
		return dbHelper.saveAfterDeleteByTransaction(saveObj, delObj);
	}

	public boolean saveAfterUpdateByTransaction(Object saveObj, Object updateObj) {
		// TODO Auto-generated method stub
		return dbHelper.saveAfterUpdateByTransaction(saveObj, updateObj);
	}

	public boolean updateAfterDeleteByTransaction(Object updateObj,
			Object delObj) {
		// TODO Auto-generated method stub
		return dbHelper.updateAfterDeleteByTransaction(updateObj, delObj);
	}

	public boolean updateAfterSaveByTransaction(Object updateObj, Object saveObj) {
		// TODO Auto-generated method stub
		return dbHelper.updateAfterSaveByTransaction(updateObj, saveObj);
	}

	public int QueryCount(String from_sql) {
		// TODO Auto-generated method stub
		return dbHelper.QueryCount(from_sql);
	}

	@Override
	public boolean multiSQLByTransaction(Map<String, Object[]> multiSQL) {
		// TODO Auto-generated method stub
		return dbHelper.multiSQLByTransaction(multiSQL);
	}

	@Override
	public boolean exeCUDByTransaction(Object addObj, Object updateObj,
			Object delObj) {
		// TODO Auto-generated method stub
		return dbHelper.exeCUDByTransaction(addObj, updateObj, delObj);
	}




}
