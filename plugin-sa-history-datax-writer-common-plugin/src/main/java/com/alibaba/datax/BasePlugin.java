package com.alibaba.datax;

import java.io.Serializable;
import java.util.*;

public abstract class BasePlugin implements Serializable {

    public abstract SAPlugin instance(Map<String,Object> param);

    public abstract static class SAPlugin{

        /**
         * 自定义处理逻辑
         * @param properties 当前行数据
         * @return 是否继续执行后续逻辑
         */
        public abstract boolean process(Map<String,Object> properties);

        /**
         * 合并map,将props中每个map中的数据放到properties中
         * 注意，需要对应插件支持多行模式
         * @param properties 主数据Map
         * @param props 待合并的map
         * @return 合并后的map,即properties
         */
        public Map<String,Object> processMultiColumn(Map<String,Object> properties,Map<String,Object> ... props){
            if(Objects.isNull(properties)){
                properties = new HashMap<>();
            }
            if(Objects.isNull(props) || props.length <= 0){
                return properties;
            }
            for (Map<String, Object> prop : props) {
                if(!Objects.isNull(prop) && !prop.isEmpty()){
                    properties.putAll(prop);
                }
            }
            return properties;
        }

        /**
         * 将多个多行Map合并到一个Map中，Map中的结构为{"__$$is_multi_column$$__":"true","data":List}，List中为合并后没每条数据
         * 注意，需要对应插件支持多行模式
         * 如果props参数中的某一个list是多个，properties中的主数据将会分裂
         * 例子 1：
         * properties ：Map{a=1,b=2}
         * props : [
         *      (Map{c=3},Map{d=4})
         * ]
         * 结果为： {
         *  "__$$is_multi_column$$__":"true",
         *  "data": [
         *      (Map{a=1,b=2,c=3}),
         *      (Map{a=1,b=2,d=4})
         *  ]
         * }
         *
         * * 例子 2：
         * properties ：Map{a=1,b=2}
         * props : [
         *      (Map{c=3},Map{d=4}),
         *      (Map{e=5},Map{f=6})
         * ]
         * 结果为： {
         *  "__$$is_multi_column$$__":"true",
         *  "data": [
         *      (Map{a=1,b=2,c=3,e=5}),
         *      (Map{a=1,b=2,d=4,e=5}),
         *      (Map{a=1,b=2,c=3,f=6}),
         *      (Map{a=1,b=2,d=4,f=6})
         *  ]
         * }
         * @param properties 一行主数据
         * @param props 待合并的多个多行数据
         * @return 合并后的map,即properties，结构为{"__$$is_multi_column$$__":"true","data":List}，List中为合并后没每条数据
         */
        public Map<String,Object> processMultiColumn(Map<String,Object> properties,List<Map<String,Object>> ... props){
            if(Objects.isNull(properties)){
                properties = new HashMap<>();
            }
            if(Objects.isNull(props) || props.length <= 0){
                return properties;
            }
            List<Map<String, Object>> result = new ArrayList<>();
            result.add(new HashMap<>(properties));
            Map<String, Object> multiOldProp = null;
            for (List<Map<String, Object>> propList : props) {
                if(Objects.isNull(propList) || propList.isEmpty()){
                    continue ;
                }
                int oldSize = result.size();
                for (int i = 0; i < oldSize; i++) {
                    multiOldProp = new HashMap<>(result.get(i));
                    result.get(i).putAll(propList.get(0));
                    for (int j = 1; j < propList.size(); j++) {
                        HashMap<String, Object> hashMap = new HashMap<>(multiOldProp);
                        hashMap.putAll(propList.get(j));
                        result.add(hashMap);
                    }
                    multiOldProp = null;
                }
            }
            properties.clear();
            properties.put("__$$is_multi_column$$__", true);
            properties.put("data", result);
            return properties;
        }
    }

}
