package com.javaband.join.impl;

import com.javaband.data.DataRow;
import com.javaband.data.JoinedDataRow;
import com.javaband.join.JoinOperation;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.stream.Stream;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

class LeftJoinOperationTest {
    JoinOperation<DataRow<Integer, String>, DataRow<Integer, String>, JoinedDataRow<Integer, String, String>> joinOperation = new LeftJoinOperation<>();

    @ParameterizedTest
    @MethodSource("data")
    void leftJoin(Collection<DataRow<Integer, String>> leftCollection,
                  Collection<DataRow<Integer, String>> rightCollection,
                  Collection<JoinedDataRow<Integer, String, String>> expected) {

        Collection<JoinedDataRow<Integer, String, String>> result = joinOperation.join(leftCollection, rightCollection);

        assertEquals(expected, result);
    }

    @Test
    void shouldThrowNullPointerException() {
        Collection<DataRow<Integer, String>> rightCollection = List.of(new DataRow<>(0, null));

        assertThrows(NullPointerException.class, () -> joinOperation.join(null, rightCollection));
    }

    public static Stream<Arguments> data() {
        return Stream.of(
                //1
                Arguments.of(
                        List.of(//left
                                new DataRow<>(1, "Ukraine"),
                                new DataRow<>(3, "Germany"),
                                new DataRow<>(5, "France")
                        ), List.of(//right
                                new DataRow<>(0, "Tokyo"),
                                new DataRow<>(3, "Berlin"),
                                new DataRow<>(4, "Budapest")
                        ), List.of(//result
                                new JoinedDataRow<>(1, "Ukraine", null),
                                new JoinedDataRow<>(3, "Germany", "Berlin"),
                                new JoinedDataRow<>(5, "France", null)
                        )
                ),
                //2
                Arguments.of(
                        Collections.emptyList(),
                        List.of(
                                new DataRow<>(0, "Tokyo"),
                                new DataRow<>(3, "Berlin")
                        ),
                        Collections.emptyList()
                ),
                //3
                Arguments.of(
                        List.of(
                                new DataRow<>(1, "Ukraine"),
                                new DataRow<>(3, "Germany"),
                                new DataRow<>(5, "France")
                        ),
                        Collections.emptyList(),
                        List.of(
                                new JoinedDataRow<>(1, "Ukraine", null),
                                new JoinedDataRow<>(3, "Germany", null),
                                new JoinedDataRow<>(5, "France", null)
                        )
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
                                new JoinedDataRow<>(1, "Ukraine", null),
                                new JoinedDataRow<>(5, "Japan", "Tokyo"),
                                new JoinedDataRow<>(8, "France", null),
                                new JoinedDataRow<>(9, "Germany", "Berlin")
                        )
                )
        );
    }
}