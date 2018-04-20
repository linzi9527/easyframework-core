package com.easyframework.core.d;

import java.sql.SQLException;
import java.util.List;
import java.util.Map;

import org.apache.log4j.Logger;

import com.easyframework.core.db.DBHelper;
import com.easyframework.core.db.PropertiesUtils;
import com.easyframework.core.db.StringUtil;
import com.easyframework.redis.api.SyncJedisHelper;
import com.easyframework.redis.core.BeanUtils;
import com.easyframework.redis.core.CommonException;


public class BaseDaoImpl implements IBaseDao {
	
	private static Logger log = Logger.getLogger(BaseDaoImpl.class);
			
	private	DBHelper dbHelper=DBHelper.getInstance();
	//数据同步到redis
	private SyncJedisHelper jedisHelper=SyncJedisHelper.getInstance();

	
	public boolean save(Object o) throws Exception {
		// TODO Auto-generated method stub
		if(PropertiesUtils.Is_Redis_Open){
			boolean	key=dbHelper.save(o);
			//数据同步到redis
			try {
				boolean jkey=jedisHelper.syncSaveTable2Redis(o, "clazz");
				if(jkey)log.info("\n["+o+"]写入数据同步到redis");
			} catch (Exception e) {
				log.error("\n写入数据同步到redis异常："+e.getMessage());
			}
			return key;
		}else{
			return dbHelper.save(o);
		}

	}

	public boolean update(Object o) throws Exception {
		// TODO Auto-generated method stub
		//return dbHelper.update(o);
		if(PropertiesUtils.Is_Redis_Open){
			boolean	key=dbHelper.update(o);
			//数据同步到redis
			try {
				boolean jkey=jedisHelper.syncUpdateTable2Redis(o, "clazz");
				if(jkey)log.info("\n["+o+"]更新数据同步到redis");
			} catch (Exception e) {
				log.error("\n更新数据同步到redis异常："+e.getMessage());
			}
			return key;
		}else{
			return dbHelper.update(o);
		}
	}


	public List<?> queryTables(Class<?> c, String[] link_name,String sql,boolean isCache) throws Exception {
		// TODO Auto-generated method stub
		return dbHelper.queryTables(c, link_name,sql,isCache);
	}


	public boolean remove(Object o) throws Exception {
		// TODO Auto-generated method stub
		//return dbHelper.remove(o);
		if(PropertiesUtils.Is_Redis_Open){
			boolean	key=dbHelper.remove(o);
			//数据同步到redis
			try {
				boolean jkey=jedisHelper.syncDeleteTable2Redis(o,"clazz");
				if(jkey)log.info("\n["+o+"]移除数据同步到redis");
			} catch (Exception e) {
				log.error("\n移除数据同步到redis异常："+e.getMessage());
			}
			return key;
		}else{
			return dbHelper.remove(o);
		}
	}


	public boolean delete(Integer id,Class<?> o) throws Exception {
		// TODO Auto-generated method stub
		//return dbHelper.delete(id, o);
		if(PropertiesUtils.Is_Redis_Open){
			boolean	key=dbHelper.delete(id, o);
			//数据同步到redis
			try {
				boolean jkey=jedisHelper.syncDeleteTable2Redis(o, "clazz", id);
				if(jkey)log.info("\n["+id+"]移除数据同步到redis");
			} catch (Exception e) {
				log.error("\n移除数据同步到redis异常："+e.getMessage());
			}
			return key;
		}else{
			return dbHelper.delete(id, o);
		}
	}


	public Object get(Integer id, Class<?> o,boolean isCache) throws Exception {
		// TODO Auto-generated method stub
		//return dbHelper.get(id, o,isCache);
		Object obj=null;
		if(PropertiesUtils.Is_Redis_Open){
			//数据同步到redis
			try {
				obj= jedisHelper.get(o, id+"");
			} catch (Exception e) {
				log.error("\n读取redis数据异常："+e.getMessage()+",智能化调整，转入mysql通道...");
			}
			if(obj!=null){
				log.info("\n在redis中命中目标...");
			}else{
				obj=dbHelper.get(id, o,isCache);
				if(obj!=null)log.info("\n在mysql中命中目标...");
			} 
			return obj;
		}else{
			return dbHelper.get(id, o,isCache);
		}
	}


	public boolean CreateTable(String sql) throws Exception {
		// TODO Auto-generated method stub
		return dbHelper.CreateTable(sql);
	}


	public List<?> queryByOracle(Class<?> o, int p1, int p2, String desc)
			throws Exception {
		// TODO Auto-generated method stub
		return dbHelper.queryByOracle(o, p1, p2, desc);
	}




	public Object load(Class<?> o, String iswhere,boolean isCache) throws Exception {
		// TODO Auto-generated method stub
		//return dbHelper.load(o, iswhere, isCache);
		Object obj=null;
		if(PropertiesUtils.Is_Redis_Open){
			//数据同步到redis
			try {
				obj= jedisHelper.queryMapBySQL(iswhere);
			} catch (Exception e) {
				log.error("\n读取redis数据异常："+e.getMessage()+",智能化调整，转入mysql通道...");
			}
			if(obj!=null){
				log.info("\n在redis中命中目标...");
			}else{
				obj=dbHelper.load(o, iswhere, isCache);
				if(obj!=null){
					log.info("\n在mysql中命中目标...");
					boolean jkey=jedisHelper.setObject(o, iswhere, obj);
					if(jkey)log.info("\n["+obj+"]对象数据同步到redis");
				}
			} 
			return obj;
		}else{
			return dbHelper.load(o, iswhere, isCache);
		}
	}

	public boolean saveByTransaction(Object[] o) throws Exception {
		// TODO Auto-generated method stub
		
		return dbHelper.saveByTransaction(o);
	}

	public Object get(String id, Class<?> o, boolean isCache) throws Exception {
		// TODO Auto-generated method stub
		//return dbHelper.get(id, o, isCache);
		Object obj=null;
		if(PropertiesUtils.Is_Redis_Open){
			//数据同步到redis
			try {
				obj= jedisHelper.get(o, id);
			} catch (Exception e) {
				log.error("\n读取redis数据异常："+e.getMessage()+",智能化调整，转入mysql通道...");
			}
			if(obj!=null){
				log.info("\n在redis中命中目标...");
			}else{
				obj=dbHelper.get(id, o,isCache);
				if(obj!=null){
					log.info("\n在mysql中命中目标...");
					boolean jkey=jedisHelper.save(obj);
					if(jkey)log.info("\n["+id+"]对象数据同步到redis");
				}
			} 
			return obj;
		}else{
			return dbHelper.get(id, o,isCache);
		}
	}
	public boolean remove(String id,Class<?> o) throws Exception {
		// TODO Auto-generated method stub
		//return dbHelper.delete(id, o);
		if(PropertiesUtils.Is_Redis_Open){
			boolean	key=dbHelper.delete(id, o);
			//数据同步到redis
			try {
				boolean jkey=jedisHelper.syncDeleteTable2Redis(o, "clazz", id);
				if(jkey)log.info("\n["+id+"]移除数据同步到redis");
			} catch (Exception e) {
				log.error("\n移除数据同步到redis异常："+e.getMessage());
			}
			return key;
		}else{
			return dbHelper.delete(id, o);
		}
	}
	
	
	@SuppressWarnings("static-access")
	public List<?> query(Class<?> o,boolean isCache) throws Exception {
		// TODO Auto-generated method stub
		//return dbHelper.find(o,isCache);
		List<?> obj=null;
		if(PropertiesUtils.Is_Redis_Open){
			//数据同步到redis
			try {
				obj= jedisHelper.query(o);
			} catch (Exception e) {
				log.error("\n读取redis数据异常："+e.getMessage()+",智能化调整，转入mysql通道...");
			}
			if(obj!=null){
				log.info("\n在redis中命中目标...");
			}else{
				obj=dbHelper.find(o, isCache);
				if(obj!=null)log.info("\n在mysql中命中目标...");
			} 
			return obj;
		}else{
			return dbHelper.find(o, isCache);
		}
	}

	@SuppressWarnings("static-access")
	public List<?> querys(Class<?> o,boolean isCache) throws Exception {
		// TODO Auto-generated method stub
		//return dbHelper.querys(o,isCache);
		List<?> obj=null;
		if(PropertiesUtils.Is_Redis_Open){
			//数据同步到redis
			try {
				obj= jedisHelper.query(o);
			} catch (Exception e) {
				log.error("\n读取redis数据异常："+e.getMessage()+",智能化调整，转入mysql通道...");
			}
			if(obj!=null&&obj.size()>0){
				log.info("\n在redis中命中目标...");
			}else{
				obj=dbHelper.querys(o, isCache);
				if(obj!=null&&obj.size()>0){
					log.info("\n在mysql中命中目标("+obj.size()+")...");
				}
			} 
			return obj;
		}else{
			return dbHelper.querys(o, isCache);
		}
	}

	public List<?> queryList(Class<?> o, String isWhere, boolean isCache)
			throws Exception {
		// TODO Auto-generated method stub
		//return dbHelper.queryList(o, isWhere, isCache);
		List<?> list=null;
		String sql="SELECT * FROM  ";
		if(PropertiesUtils.Is_Redis_Open){
			//数据同步到redis

			if(!isWhere.trim().toLowerCase().startsWith("select")){
				sql=sql+StringUtil.tableName(o)+" "+isWhere;
			}
			
			try {
				list= (List<?>) jedisHelper.queryStrListBySQL(o, sql);
			} catch (Exception e) {
				log.error("\n读取redis数据异常："+e.getMessage()+",智能化调整，转入mysql通道...");
			}
			if(list!=null&&list.size()>0){
				log.info("\n在redis中命中目标...");
			}else{
				list=dbHelper.queryList(o, isWhere, isCache);
				if(list!=null&&list.size()>0){
					log.info("\n在mysql中命中目标("+list.size()+")...");
					boolean key=jedisHelper.setStrListBySQL(sql, list);
					if(key)log.info("\n在mysql中命中目标-->已同步到redis中.");
				}
			} 
			return list;
		}else{
			return dbHelper.queryList(o, isWhere, isCache);
		}
	}
	

	public List<?> query(Class<?> c, String sql, boolean isCache)
			throws Exception {
		// TODO Auto-generated method stub
		//return  dbHelper.query(c, sql, isCache);
		List<?> list=null;
		if(PropertiesUtils.Is_Redis_Open){
			//数据同步到redis
			try {
				list= (List<?>) jedisHelper.queryStrListBySQL(c, sql);
			} catch (Exception e) {
				log.error("\n读取redis数据异常："+e.getMessage()+",智能化调整，转入mysql通道...");
			}
			if(list!=null&&list.size()>0){
				log.info("\n在redis中命中目标...");
			}else{
				list=dbHelper.query(c, sql, isCache);
				if(list!=null&&list.size()>0){
					log.info("\n在mysql中命中目标("+list.size()+")...");
					boolean key=jedisHelper.setStrListBySQL(sql, list);
					if(key)log.info("\n在mysql中命中目标-->已同步到redis中.");
				}
			} 
			return list;
		}else{
			return dbHelper.query(c, sql, isCache);
		}
	}

	public Map<String, Object> queryMap(String sql) {
		// TODO Auto-generated method stub
		// 动态bean使用map<k,n>来代替
		//return dbHelper.queryMap(sql);
		Map<String, Object> obj=null;
		if(PropertiesUtils.Is_Redis_Open){
			//数据同步到redis
			try {
				obj=(Map<String, Object>) jedisHelper.queryMapBySQL(sql);
			} catch (Exception e) {
				log.error("\n读取redis数据异常："+e.getMessage()+",智能化调整，转入mysql通道...");
			}
			if(obj!=null&&obj.size()>0){
				log.info("\n在redis中命中目标...");
			}else{
				obj=dbHelper.queryMap(sql);
				if(obj!=null&&obj.size()>0){
					log.info("\n在mysql中命中目标("+obj+")...");
					boolean key=jedisHelper.setMapBySQL(sql, obj);
					if(key)log.info("\n在mysql中命中目标-->已同步到redis中.");
				}
			} 
			return obj;
		}else{
			return dbHelper.queryMap(sql);
		}
	}

	public List<Map<String, Object>> queryListMap(String sql) {
		// TODO Auto-generated method stub
		//return dbHelper.queryListMap(sql);
		List<Map<String, Object>> list=null;
		if(PropertiesUtils.Is_Redis_Open){
			//数据同步到redis
			try {
				list=jedisHelper.queryListMapBySQL(sql);
			} catch (Exception e) {
				log.error("\n读取redis数据异常："+e.getMessage()+",智能化调整，转入mysql通道...");
			}
			if(list!=null&&list.size()>0){
				log.info("\n在redis中命中目标...");
			}else{
				list=dbHelper.queryListMap(sql);
				if(list!=null&&list.size()>0){
					log.info("\n在mysql中命中目标("+list.size()+")...");
					boolean key=jedisHelper.setListMapBySQL(sql, list);
					if(key)log.info("\n在mysql中命中目标-->已同步到redis中.");
				}
			} 
			return list;
		}else{
			return dbHelper.queryListMap(sql);
		}
	}
	
	public <T>T list_Map_String_Object2ListClazz(List<Map<String, Object>> data,Class<T> bean) {
		 List<T> list=null;
		 BeanUtils<T> beanUtils = new BeanUtils<T>();
			try {
				 list=beanUtils.ListMap2JavaBean(data,bean);
	        } catch (CommonException e) {
	            e.printStackTrace();
	        }
		 
		 return (T) list;
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

	public boolean saveUpdate(Object o) throws Exception {
		// TODO Auto-generated method stub
		//return dbHelper.saveUpdate(o);
		if(PropertiesUtils.Is_Redis_Open){
			boolean	key=dbHelper.saveUpdate(o);
			//数据同步到redis
			try {
				boolean jkey=jedisHelper.syncSaveTable2Redis(o, "clazz");
				if(jkey)log.info("\n["+o+"]写入数据同步到redis");
			} catch (Exception e) {
				log.error("\n写入数据同步到redis异常："+e.getMessage());
			}
			return key;
		}else{
			return dbHelper.saveUpdate(o);
		}
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

	@Override
	public boolean removeByTransaction(String[] ids, Class<?> o) {
		// TODO Auto-generated method stub
		return dbHelper.removeByTransaction(ids, o);
	}



}
