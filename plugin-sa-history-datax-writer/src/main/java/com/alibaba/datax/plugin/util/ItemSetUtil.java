package com.alibaba.datax.plugin.util;

import com.alibaba.datax.plugin.KeyConstant;
import com.sensorsdata.analytics.javasdk.SensorsAnalytics;
import com.sensorsdata.analytics.javasdk.exceptions.InvalidArgumentException;
import lombok.extern.slf4j.Slf4j;

import java.util.Map;
import java.util.concurrent.atomic.LongAdder;

@Slf4j
public class ItemSetUtil {

    public static LongAdder SEND_SA_COUNT = new LongAdder();

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

        try {
            sa.itemSet(itemItemType,itemId,properties);
//            java sdk 3.2.20的写法
//            ItemRecord addRecord = ItemRecord.builder().setItemId(itemId).setItemType(itemItemType)
//                    .addProperties(properties)
//                    .build();
//            sa.itemSet(addRecord);
            sa.flush();
            SEND_SA_COUNT.increment();
        } catch (Exception e) {
            log.info("item Exception: {}", e);
            e.printStackTrace();
        }

    }

}
