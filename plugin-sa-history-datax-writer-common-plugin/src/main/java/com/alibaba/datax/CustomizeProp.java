package com.alibaba.datax;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

public class CustomizeProp {

    private CustomizeProp(){}

    private final static Map<String,Object> prop = new ConcurrentHashMap<>();

    public static Map<String,Object> prop(){
        return prop;
    }

    public static void set(String key,Object value){
        prop.put(key,value);
    }

    public static Object get(String key){
        return prop.get(key);
    }

    public static Object getOrDefault(String key,Object defaultValue){
        return prop.getOrDefault(key,defaultValue);
    }
}
