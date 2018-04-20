package com.easyframework.core.db;

import java.io.File;
import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.util.Properties;

import javax.servlet.ServletContext;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpSession;

import org.apache.log4j.Logger;
import org.springframework.web.context.ContextLoader;
import org.springframework.web.context.WebApplicationContext;
import org.springframework.web.context.request.RequestContextHolder;
import org.springframework.web.context.request.ServletRequestAttributes;

public class SysUtils {


	private static Logger logger = Logger.getLogger(SysUtils.class);
	
	private static WebApplicationContext context = null;

	static {
		if (context == null) {
			context = ContextLoader.getCurrentWebApplicationContext();
		}
	}

	public static ServletContext getServletContext() {

		return context.getServletContext();
	}
	
	public static HttpSession getSession() { 
		  HttpSession session = null; 
		  try { 
		    session = getRequest().getSession(); 
		  } catch (Exception e) {
			  logger.error("获取session-异常:"+e.getMessage());
		  } 
		    return session; 
	} 
		  
	public static HttpServletRequest getRequest() { 
		ServletRequestAttributes attrs =null;
		try {
			 attrs =(ServletRequestAttributes) RequestContextHolder.getRequestAttributes(); 
			 return attrs.getRequest();
		} catch (Exception e) {
			 logger.error("获取Request-异常:"+e.getMessage());
		} 
		return null;
	} 
	
	public static Properties getProperties() {
		Properties properties = new Properties();
		try {
			properties.load(SysUtils.class.getResourceAsStream("/db.properties"));
		} catch (IOException e) {
			logger.error("获取Properties文件-异常:"+e.getMessage());
		}
		return properties;
	}
	
	public static String getPath(String key) throws UnsupportedEncodingException
	{
		String path = getServletContext().getRealPath(new String(getProperties().getProperty(key).getBytes("ISO8859-1"),"UTF-8"));
		File file = new File(path);
		if(file != null&&file.isFile())
		{
			return file.getAbsolutePath();
		}
		return null;
	}
	
}
