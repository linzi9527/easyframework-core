package com.easyframework.core.db;

import java.io.Serializable;
import java.lang.reflect.ParameterizedType;
import java.lang.reflect.Type;
import java.util.List;

import com.google.gson.Gson;

/**
 * 	＃第三种：
	{"success":true,"data":[{"starLevel":4,"remarkCotnent":"评价方未及时做出评价，系统默认满意！","remarkTime":"2013-02-27 07:21:48","explainContent":"","postMemberId":"y**f","tpLogoURL":"http://i04.c.aliimg.com/cms/upload/2012/186/684/486681_1232736939.png"},{"starLevel":4,"remarkCotnent":"评价方未及时做出评价，系统默认满意！","remarkTime":"2013-02-27 07:21:48","explainContent":"","postMemberId":"y**f","tpLogoURL":"http://i04.c.aliimg.com/cms/upload/2012/186/684/486681_1232736939.png"}]}

 * @author Administrator
 *
 * @param <T>
 */
public class CommonJson3List<T> implements Serializable {

    /**
     *
     */
    private static final long serialVersionUID = -369558847578246550L;

    /**
     * 是否成功
     */
    private Boolean success;

    /**
     * 数据
     */
    private List<T> data;

    public Boolean getSuccess() {
        return success;
    }

    public void setSuccess(Boolean success) {
        this.success = success;
    }

    public List<T> getData() {
        return data;
    }

    public void setData(List<T> data) {
        this.data = data;
    }

    public static CommonJson3List fromJson(String json, Class clazz) {
        Gson gson = new Gson();
        Type objectType = type(CommonJson3List.class, clazz);
        return gson.fromJson(json, objectType);
    }

    public String toJson(Class<T> clazz) {
        Gson gson = new Gson();
        Type objectType = type(CommonJson3List.class, clazz);
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
