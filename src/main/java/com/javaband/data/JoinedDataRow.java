package com.javaband.data;

import lombok.AllArgsConstructor;
import lombok.Data;

@Data
@AllArgsConstructor
public class JoinedDataRow<K extends Comparable<K>, V1, V2> {
    private K key;
    private V1 value1;
    private V2 value2;
}
