package com.javaband.join.impl;

import com.javaband.data.DataRow;
import com.javaband.data.JoinedDataRow;
import com.javaband.join.JoinOperation;
import org.junit.jupiter.api.Order;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.stream.Stream;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

class InnerJoinOperationTest {
    JoinOperation<DataRow<Integer, String>, DataRow<Integer, String>, JoinedDataRow<Integer, String, String>> joinOperation = new InnerJoinOperation<>();

    @ParameterizedTest
    @MethodSource("data")
    @Order(1)
    void innerJoin(Collection<DataRow<Integer, String>> leftCollection,
                   Collection<DataRow<Integer, String>> rightCollection,
                   Collection<JoinedDataRow<Integer, String, String>> expected) {

        Collection<JoinedDataRow<Integer, String, String>> result = joinOperation.join(leftCollection, rightCollection);

        assertEquals(expected, result);
    }

    @Test
    @Order(2)
    void shouldThrowNullPointerException() {

        assertThrows(NullPointerException.class, () -> joinOperation.join(null, null));
    }

    @Test
    @Order(3)
    void whenHighVolumeInput() {
        long expectedSize = 899000;
        Collection<DataRow<Integer, String>> left = new ArrayList<>();
        for (int i = 0; i < 1000000; i++) {
            left.add(new DataRow<>(i, "a" + i));
        }

        Collection<DataRow<Integer, String>> right = new ArrayList<>();
        for (int i = 1000; i < 900000; i++) {
            right.add(new DataRow<>(i, "b" + i));
        }

        long start = System.currentTimeMillis();
        Collection<JoinedDataRow<Integer, String, String>> result = joinOperation.join(left, right);
        long end = System.currentTimeMillis();

        //to compare
        joinOperation = new InnerJoinOperationV2<>();

        long startV2 = System.currentTimeMillis();
        Collection<JoinedDataRow<Integer, String, String>> result1 = joinOperation.join(left, right);
        long endV2 = System.currentTimeMillis();

        System.out.println(end - start + " milliseconds");
        System.out.println(endV2 - startV2 + " milliseconds");

        assertTrue(end - start < endV2 - startV2);
        assertEquals(result.size(), expectedSize);
        assertEquals(result1.size(), expectedSize);
    }

    public static Stream<Arguments> data() {
        return Stream.of(
                //1
                Arguments.of(List.of(
                        new DataRow<>(1, "Ukraine"),
                        new DataRow<>(3, "Germany"),
                        new DataRow<>(5, "France")
                ), List.of(
                        new DataRow<>(0, "Tokyo"),
                        new DataRow<>(3, "Berlin"),
                        new DataRow<>(4, "Budapest")
                ), List.of(
                        new JoinedDataRow<>(3, "Germany", "Berlin")
                )),
                //2
                Arguments.of(
                        List.of(
                                new DataRow<>(1, "Ukraine"),
                                new DataRow<>(3, "Germany")
                        ),
                        Collections.emptyList(),
                        Collections.emptyList()
                ),
                //3
                Arguments.of(
                        Collections.emptyList(),
                        List.of(
                                new DataRow<>(0, "Tokyo"),
                                new DataRow<>(3, "Berlin"),
                                new DataRow<>(4, "Budapest")
                        ),
                        Collections.emptyList()
                ),
                //4
                Arguments.of(
                        List.of(
                                new DataRow<>(1, "Ukraine"),
                                new DataRow<>(5, "Japan"),
                                new DataRow<>(8, "France"),
                                new DataRow<>(9, "Germany")
                        ), List.of(
                                new DataRow<>(5, "Tokyo"),
                                new DataRow<>(9, "Berlin"),
                                new DataRow<>(11, "Budapest")
                        ), List.of(
                                new JoinedDataRow<>(5, "Japan", "Tokyo"),
                                new JoinedDataRow<>(9, "Germany", "Berlin")
                        )
                )
        );
    }
}