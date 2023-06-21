package com.alibaba.datax.plugin.util;

import cn.hutool.core.util.StrUtil;
import com.alibaba.datax.plugin.KeyConstant;
import com.sensorsdata.analytics.javasdk.SensorsAnalytics;
import lombok.extern.slf4j.Slf4j;

import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.LongAdder;

@Slf4j
public class ItemDeleteUtil {

    public static LongAdder SEND_SA_COUNT = new LongAdder();
    public static LongAdder  FILTER_COUNT = new LongAdder();
    public static LongAdder  ERROR_COUNT = new LongAdder();

    public static void process(SensorsAnalytics sa, Map<String, Object> properties) {
        String itemItemType = (String) properties.get(KeyConstant.ITEM_ITEM_TYPE);
        Boolean itemTypeIsColumn = Boolean.parseBoolean(properties.get(KeyConstant.ITEM_TYPE_IS_COLUMN).toString());
        String itemItemIdColumn = (String) properties.get(KeyConstant.ITEM_ITEM_ID_COLUMN);
        String itemId = String.valueOf(properties.get(itemItemIdColumn));

        String typeTmp = itemItemType;
        itemItemType = itemTypeIsColumn ? String.valueOf(properties.get(itemItemType)) : itemItemType;
        properties.remove(KeyConstant.ITEM_ITEM_TYPE);
        properties.remove(KeyConstant.ITEM_TYPE_IS_COLUMN);
        properties.remove(KeyConstant.ITEM_ITEM_ID_COLUMN);
        if (itemTypeIsColumn) {
            properties.remove(typeTmp);
        }
        properties.remove(itemItemIdColumn);
        if(StrUtil.isEmpty(itemItemType) || StrUtil.isEmpty(itemId)){
            FILTER_COUNT.increment();
            return;
        }

        try {
            sa.itemDelete(itemItemType,itemId,null);
//            java sdk 3.2.20的写法
//            ItemRecord addRecord = ItemRecord.builder().setItemId(itemId).setItemType(itemItemType)
//                    .addProperties(properties)
//                    .build();
//            sa.itemSet(addRecord);
            sa.flush();
            SEND_SA_COUNT.increment();
            Set<Map.Entry<String, LongAdder>> entries = StatisticsUtil.getAllNullCountColumn().entrySet();
            for (Map.Entry<String, LongAdder> entry : entries) {
                String columnName = entry.getKey();
                LongAdder longAdder = entry.getValue();
                if (NullUtil.isNullOrBlank(properties.getOrDefault(columnName, null))) {
                    longAdder.increment();
                }
            }
        } catch (Exception e) {
            log.error("item Exception:", e);
            ERROR_COUNT.increment();
            log.info("Item Exception happened! data：{}",properties);
        }

    }

}
