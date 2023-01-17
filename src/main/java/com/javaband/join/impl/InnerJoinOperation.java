package com.javaband.join.impl;

import com.javaband.data.DataRow;
import com.javaband.data.JoinedDataRow;
import com.javaband.join.JoinOperation;

import java.util.Collection;
import java.util.stream.Collectors;

public class InnerJoinOperation<K extends Comparable<K>, V1, V2> implements JoinOperation<DataRow<K, V1>, DataRow<K, V2>, JoinedDataRow<K, V1, V2>> {
    private final JoinOperation<DataRow<K, V1>, DataRow<K, V2>, JoinedDataRow<K, V1, V2>> joinOperation;

    public InnerJoinOperation() {
        joinOperation = new RightJoinOperation<>();//left or right
    }

    @Override
    public Collection<JoinedDataRow<K, V1, V2>> join(Collection<DataRow<K, V1>> leftCollection, Collection<DataRow<K, V2>> rightCollection) {
        return joinOperation.join(leftCollection, rightCollection).parallelStream()
                .filter(joinedDataRow -> joinedDataRow.getValue1() != null && joinedDataRow.getValue2() != null)
                .collect(Collectors.toList());
    }
}
