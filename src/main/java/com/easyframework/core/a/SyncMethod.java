package com.easyframework.core.a;

import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;


@Retention(RetentionPolicy.RUNTIME)
@Target(ElementType.TYPE)
public @interface SyncMethod {

	String         name() default "redis";//当前方法名称 
	boolean isRedisSync() default false;  //当前方法是否开开启redis数据同步
	int         dbIndex() default 0;      //数据写入数据库编号如:db0、db1...
	long     dataExpire() default 60;     //数据过期时间,默认60s
	public enum ObjTypes{STRING,LIST,CLAZZ,MAP,OBJECT};
	ObjTypes       type() default ObjTypes.STRING;//数据同步类型：String、list、object、clazz、map等
}
