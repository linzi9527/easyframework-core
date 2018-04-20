package com.easyframework.core.db;

import java.security.MessageDigest;

public class MD5Util {

	/**
	 * 将密码进行加密
	 * @param s
	 * @return
	 */
	public final static String MD5(String s) {
		  char hexDigits[] = { '0', '1', '2', '3', '4', '5', '6', '7', '8',

		                       '9', 'a', 'b', 'c', 'd', 'e', 'f' };
		  try {
		     byte[] strTemp = s.getBytes();
		     //使用MD5创建MessageDigest对象
		     MessageDigest mdTemp = MessageDigest.getInstance("MD5");
		     mdTemp.update(strTemp);
		     byte[] md = mdTemp.digest();
		     int j = md.length;
		     char str[] = new char[j * 2];


		     int k = 0;
		     for (int i = 0; i < j; i++) {
		          byte b = md[i];
		         //System.out.println((int)b);
		        //将没个数(int)b进行双字节加密
		        str[k++] = hexDigits[b >> 4 & 0xf];
		        str[k++] = hexDigits[b & 0xf];
		   }


		   return new String(str);
		  } catch (Exception e) {return null;}
		}
	
	/*  //测试
	public static void main(String[] args) {
	  System.out.println("admin的MD5加密后：\n"+MD5Util.MD5("6557901"));
	  System.out.println("http://www.baidu.com/的MD5加密后：\n"+MD5Util.MD5("http://www.baidu.com/"));
	}
*/	
}
