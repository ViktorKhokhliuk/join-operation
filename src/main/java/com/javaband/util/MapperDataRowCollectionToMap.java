package com.javaband.util;

import com.javaband.data.DataRow;

import java.util.Collection;
import java.util.HashMap;
import java.util.Map;

public class MapperDataRowCollectionToMap {

    public static <K extends Comparable<K>, V> Map<K, V> map(Collection<DataRow<K, V>> collection) {
        Map<K, V> dataRowMap = new HashMap<>();
        collection.forEach(dataRow -> dataRowMap.put(dataRow.getKey(), dataRow.getValue()));
        return dataRowMap;
    }
}
