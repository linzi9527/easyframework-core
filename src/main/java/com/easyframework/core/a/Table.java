package com.easyframework.core.a;

import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

@Retention(RetentionPolicy.RUNTIME)
@Target(ElementType.TYPE)
public @interface Table {
	//映射的数据库对应表名
	public String name();
	//策略：按照数据库表的字段为参照，按照vo中属性为参照
	public enum policy{TABLE,VO};
	public policy type() default policy.TABLE;
}
