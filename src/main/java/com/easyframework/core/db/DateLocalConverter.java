package com.easyframework.core.db;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.Locale;

import org.apache.commons.beanutils.Converter;

public class DateLocalConverter implements Converter {
	
	/*public static void main(String[] a) {
	
		// 私用的指建转换器
		ConvertUtilsBean convertUtils = new ConvertUtilsBean();// java自动转换的工具类
		DateConvert dateConverter = new DateConvert();// 实力㈠个日期转换类
		convertUtils.register(dateConverter, Date.class);// 注册㈠个日期类
		convertUtils.register(dateConverter, String.class);// 注册㈠个字符类
		BeanUtilsBean beanUtils = new BeanUtilsBean(convertUtils,new PropertyUtilsBean());// 将这个转换工具类加载到beanUtils属性中
		{
			D2 d2 = new D2();
			try {
				beanUtils.copyProperties(d2, d1);
				System.out.println(d2.getDate());
			} catch (Exception e) {
				System.err.println("1.没指建时间转换器");
			}
		}*/

		// 公用的那个因为没有指建转换器
		/*{
			D2 d2 = new D2();
			try {
				BeanUtils.copyProperties(d2, d1);
				System.out.println(d2.getDate());
			} catch (Exception e) {
				System.err.println("2.没指建时间转换器");
			}
		}*/

		// 公用的指建转换器
		/*{
			ConvertUtils.register(dateConverter, Date.class);// 注册㈠个日期类
			ConvertUtils.register(dateConverter, String.class);// 注册㈠个字符类
			D2 d2 = new D2();
			try {
				BeanUtils.copyProperties(d2, d1);
				System.out.println(d2.getDate());
			} catch (Exception e) {
				System.out.println("3.没指建时间转换器");
			}
		}
	}*/

	//@Override
	@SuppressWarnings("rawtypes")
	public Object convert(Class type, Object value) {
		if (value == null) {
			return null;
		}
		if (value instanceof Date) {
			return value;
		}
		if (value instanceof Long) {
			Long longValue = (Long) value;
			return new Date(longValue.longValue());
		}
		if (value instanceof String) {
			/*Date endTime = null;
			try {
				endTime = DateUtils.parseDate(value.toString(), new String[] {
						"yyyy-MM-dd HH:mm:ss.SSS", "yyyy-MM-dd HH:mm:ss",
						"yyyy-MM-dd HH:mm" });
			} catch (ParseException e) {
				e.printStackTrace();
			}
			return endTime;*/
			SimpleDateFormat sdf1 = new SimpleDateFormat ("EEE MMM dd HH:mm:ss Z yyyy", Locale.UK);
		       try
		       {
		       	    Date d=sdf1.parse(value.toString());
		           //SimpleDateFormat sdf=new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
		          // String sDate=sdf.format(d);
		           //System.out.println(sDate);
		           return d;
		       }
		       catch (ParseException e)
		       {
		           e.printStackTrace();
		       }
		}
		return null;
	}
	
	//String x = "Mon Mar 02 13:57:49 CST 2015";
	/*public void convert(String date) {
	       SimpleDateFormat sdf1 = new SimpleDateFormat ("EEE MMM dd HH:mm:ss Z yyyy", Locale.UK);
	       try
	       {
	       	    Date d=sdf1.parse(date);
	           SimpleDateFormat sdf=new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
	           String sDate=sdf.format(d);
	           //System.out.println(sDate);
	       }
	       catch (ParseException e)
	       {
	           e.printStackTrace();
	       }
	}*/
	
	
	
	public static void main(String[] args) {
		DateLocalConverter d=new DateLocalConverter();
		Object o=d.convert(null,"Sat Nov 19 15:17:02 CST 2016");
		 SimpleDateFormat sdf=new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
         String sDate=sdf.format(o);
         System.out.println(sDate);

	}

}
