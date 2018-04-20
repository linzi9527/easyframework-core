package com.easyframework.core.a;

import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

/**
 * 通过注解来完成事物编程
 * @author Administrator
 *
 */


@Target(ElementType.METHOD)
@Retention(RetentionPolicy.RUNTIME)
public @interface Method {

	 String saveByTransaction()   default "saveAuto";
	 
	 String updateByTransaction() default "updateAuto";
	 
	 String deleteByTransaction() default "deleteAuto";
}
