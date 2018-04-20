package com.easyframework.core.db;


import java.lang.reflect.ParameterizedType;
import java.lang.reflect.Type;
import java.io.Serializable;

import com.google.gson.Gson;

/**
 * 
 * 
 * 
 *  ＃第一种：
	{"success":true,"data":{"averageStarLevel":4.7,"remarkCount":10}}
	＃第二种：
	{"success":true,"data":{"page":10,"pageCount":29,
			                 "list":[{"starLevel":4,"remarkCotnent":"评价方未及时做出评价，系统默认满意！","remarkTime":"2013-02-27 07:21:48","explainContent":"","postMemberId":"y**f","tpLogoURL":"http://i04.c.aliimg.com/cms/upload/2012/186/684/486681_1232736939.png"},{"starLevel":4,"remarkCotnent":"评价方未及时做出评价，系统默认满意！","remarkTime":"2013-02-27 07:21:48","explainContent":"","postMemberId":"y**f","tpLogoURL":"http://i04.c.aliimg.com/cms/upload/2012/186/684/486681_1232736939.png"}],"statistics":{"star5":20,"star4":40,"star3":30,"star2":10,"star1":0}}}
	＃第三种：
	{"success":true,"data":[{"starLevel":4,"remarkCotnent":"评价方未及时做出评价，系统默认满意！","remarkTime":"2013-02-27 07:21:48","explainContent":"","postMemberId":"y**f","tpLogoURL":"http://i04.c.aliimg.com/cms/upload/2012/186/684/486681_1232736939.png"},{"starLevel":4,"remarkCotnent":"评价方未及时做出评价，系统默认满意！","remarkTime":"2013-02-27 07:21:48","explainContent":"","postMemberId":"y**f","tpLogoURL":"http://i04.c.aliimg.com/cms/upload/2012/186/684/486681_1232736939.png"}]}
 *  基本相同的JSON结构，所以想定义一种通用模型对应到此结构。但是，data中的数据类型不一致。
 *  如第一种是简单对象，第二种是对象中嵌套数组，第三种是List。针对data数据类型不一致的情况，使用泛型来解决。
 * @author Administrator
 *
 * @param <T>
 */

public class CommonJson<T> implements Serializable {

    /**
     *
     */
    private static final long serialVersionUID = -3440061414071692254L;

    /**
     * 是否成功
     */
    private Boolean success;

    /**
     * 数据
     */
    private T data;

    public Boolean getSuccess() {
        return success;
    }

    public void setSuccess(Boolean success) {
        this.success = success;
    }

    public T getData() {
        return data;
    }

    public void setData(T data) {
        this.data = data;
    }
    
    
    
    
    /**
     * 可以满足第一种和第二种JSON的解析
     * @param json
     * @param clazz
     * @return
     */
    
    
    public static CommonJson fromJson(String json, Class clazz) {
        Gson gson = new Gson();
        Type objectType = type(CommonJson.class, clazz);
        return gson.fromJson(json, objectType);
    }

    public String toJson(Class<T> clazz) {
        Gson gson = new Gson();
        Type objectType = type(CommonJson.class, clazz);
        return gson.toJson(this, objectType);
    }

    static ParameterizedType type(final Class raw, final Type... args) {
        return new ParameterizedType() {
            public Type getRawType() {
                return raw;
            }

            public Type[] getActualTypeArguments() {
                return args;
            }

            public Type getOwnerType() {
                return null;
            }
        };
    }
    
    
    
    
    
    
}