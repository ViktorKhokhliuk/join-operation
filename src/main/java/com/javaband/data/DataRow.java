package com.javaband.data;

import lombok.AllArgsConstructor;
import lombok.Data;

@Data
@AllArgsConstructor
public class DataRow<K extends Comparable<K>, V> {
    private K key;
    private V value;
}
