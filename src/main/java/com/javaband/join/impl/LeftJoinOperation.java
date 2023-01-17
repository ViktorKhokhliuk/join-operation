package com.javaband.join.impl;

import com.javaband.data.DataRow;
import com.javaband.data.JoinedDataRow;
import com.javaband.join.JoinOperation;
import com.javaband.util.MapperDataRowCollectionToMap;

import java.util.Collection;
import java.util.Map;
import java.util.stream.Collectors;

public class LeftJoinOperation<K extends Comparable<K>, V1, V2> implements JoinOperation<DataRow<K, V1>, DataRow<K, V2>, JoinedDataRow<K, V1, V2>> {

    @Override
    public Collection<JoinedDataRow<K, V1, V2>> join(Collection<DataRow<K, V1>> leftCollection, Collection<DataRow<K, V2>> rightCollection) {
        Map<K, V2> rightDataRowMap = MapperDataRowCollectionToMap.map(rightCollection);

        return leftCollection.parallelStream()
                .map(leftDataRow -> new JoinedDataRow<>(leftDataRow.getKey(), leftDataRow.getValue(), rightDataRowMap.get(leftDataRow.getKey())))
                .collect(Collectors.toList());
    }
}
