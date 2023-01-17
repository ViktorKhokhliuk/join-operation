package com.javaband.join.impl;

import com.javaband.data.DataRow;
import com.javaband.data.JoinedDataRow;
import com.javaband.join.JoinOperation;
import com.javaband.util.MapperDataRowCollectionToMap;

import java.util.Collection;
import java.util.Map;
import java.util.stream.Collectors;

public class RightJoinOperation<K extends Comparable<K>, V1, V2> implements JoinOperation<DataRow<K, V1>, DataRow<K, V2>, JoinedDataRow<K, V1, V2>> {

    @Override
    public Collection<JoinedDataRow<K, V1, V2>> join(Collection<DataRow<K, V1>> leftCollection, Collection<DataRow<K, V2>> rightCollection) {
        Map<K, V1> leftDataRowMap = MapperDataRowCollectionToMap.map(leftCollection);

        return rightCollection.parallelStream()
                .map(rightDataRow -> new JoinedDataRow<>(rightDataRow.getKey(), leftDataRowMap.get(rightDataRow.getKey()), rightDataRow.getValue()))
                .collect(Collectors.toList());
    }
}
