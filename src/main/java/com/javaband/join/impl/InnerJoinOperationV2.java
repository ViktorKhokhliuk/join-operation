package com.javaband.join.impl;

import com.javaband.data.DataRow;
import com.javaband.data.JoinedDataRow;
import com.javaband.join.JoinOperation;
import com.javaband.util.MapperDataRowCollectionToMap;

import java.util.Collection;
import java.util.Map;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.Stream;

/*
Slower
 */
public class InnerJoinOperationV2<K extends Comparable<K>, V1, V2> implements JoinOperation<DataRow<K, V1>, DataRow<K, V2>, JoinedDataRow<K, V1, V2>> {

    @Override
    public Collection<JoinedDataRow<K, V1, V2>> join(Collection<DataRow<K, V1>> leftCollection, Collection<DataRow<K, V2>> rightCollection) {
        Map<K, V1> leftMap = MapperDataRowCollectionToMap.map(leftCollection);
        Map<K, V2> rightMap = MapperDataRowCollectionToMap.map(rightCollection);

        return Stream.concat(leftMap.keySet().stream(), rightMap.keySet().stream())
                .collect(Collectors.groupingBy(Function.identity(), Collectors.counting()))
                .entrySet()
                .stream()
                .filter(entry -> entry.getValue() > 1)
                .map(entry -> new JoinedDataRow<>(entry.getKey(), leftMap.get(entry.getKey()), rightMap.get(entry.getKey())))
                .collect(Collectors.toList());
    }
}
