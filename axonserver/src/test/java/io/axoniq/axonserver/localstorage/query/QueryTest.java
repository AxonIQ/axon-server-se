/*
 * Copyright (c) 2017-2019 AxonIQ B.V. and/or licensed to AxonIQ B.V.
 * under one or more contributor license agreements.
 *
 *  Licensed under the AxonIQ Open Source License Agreement v1.0;
 *  you may not use this file except in compliance with the license.
 *
 */

package io.axoniq.axonserver.localstorage.query;

import com.fasterxml.jackson.databind.ObjectMapper;
import io.axoniq.axonserver.queryparser.EventStoreQueryParser;
import io.axoniq.axonserver.queryparser.Query;
import io.axoniq.axonserver.grpc.MetaDataValue;
import io.axoniq.axonserver.grpc.SerializedObject;
import io.axoniq.axonserver.grpc.event.Event;
import io.axoniq.axonserver.grpc.event.EventWithToken;
import io.axoniq.axonserver.localstorage.query.result.DefaultQueryResult;
import io.axoniq.axonserver.localstorage.query.result.EventExpressionResult;
import org.junit.*;

import java.text.ParseException;
import java.util.ArrayList;
import java.util.List;
import java.util.stream.Stream;

import static org.junit.Assert.*;

/**
 * @author Marc Gathier
 */
public class QueryTest {

    private Stream<EventWithToken> eventStream;
    private ObjectMapper objectMapper;

    @Before
    public void setup() {
        List<EventWithToken> events = new ArrayList<>();
        objectMapper = new ObjectMapper();

        events.add(EventWithToken.newBuilder().setEvent(Event.newBuilder()
                                                                           .setAggregateType("Type1")
                                                                           .setAggregateSequenceNumber(1L)
                                                                           .setPayload(SerializedObject.newBuilder()
                                                                                                       .setType("PayloadType1")
                                                                                                       .build())
                                                                           .putMetaData("meta-key", MetaDataValue.newBuilder().setTextValue("text-value").build())
                                                                           .putMetaData("otherkey", MetaDataValue.newBuilder().setNumberValue(42L).build())
                                                                           .build()).setToken(0).build());
        events.add(EventWithToken.newBuilder().setEvent(Event.newBuilder()
                                                                           .setAggregateType("Type2")
                                                                           .setAggregateSequenceNumber(11L)
                                                                           .setPayload(SerializedObject.newBuilder()
                                                                                                       .setType("PayloadType1")
                                                                                                       .build())
                                                                           .putMetaData("meta-key", MetaDataValue.newBuilder().setTextValue("text-value2").build())
                                                                           .putMetaData("otherkey", MetaDataValue.newBuilder().setNumberValue(42L).build())
                                                             .build()).setToken(1).build());
        events.add(EventWithToken.newBuilder().setEvent(Event.newBuilder()
                                                                           .setAggregateType("Type1")
                                                                           .setAggregateSequenceNumber(2L)
                                                                           .setPayload(SerializedObject.newBuilder()
                                                                                                       .setType("PayloadType2")
                                                                                                       .build())
                                                                           .putMetaData("meta-key", MetaDataValue.newBuilder().setTextValue("text-value2").build())
                                                                           .putMetaData("otherkey", MetaDataValue.newBuilder().setNumberValue(1L).build())
                                                             .build()).setToken(2).build());
        events.add(EventWithToken.newBuilder().setEvent(Event.newBuilder()
                                                                           .setAggregateType("Type3")
                                                                           .setAggregateSequenceNumber(3L)
                                                                           .setPayload(SerializedObject.newBuilder()
                                                                                                       .setType("PayloadType1")
                                                                                                       .build())
                                                                           .putMetaData("otherkey", MetaDataValue.newBuilder().setNumberValue(1L).build())
                                                             .build()).setToken(3).build());
        events.add(EventWithToken.newBuilder().setEvent(Event.newBuilder()
                                                                           .setAggregateType("Type1")
                                                                           .setAggregateSequenceNumber(12L)
                                                                           .setPayload(SerializedObject.newBuilder()
                                                                                                       .setType("PayloadType1")
                                                                                                       .build())
                                                                           .putMetaData("meta-key", MetaDataValue.newBuilder().setTextValue("text-value").build())
                                                             .build()).setToken(4).build());

        eventStream = events.stream();
    }

    @Test
    public void groupByExpression() {
        List<QueryResult> results = executeQuery("groupby([payloadType, aggregateType], count(aggregateSequenceNumber))");
        results.forEach(System.out::println);
        assertEquals(5, results.size());
        // TODO: Validate result contents
    }

    @Test
    public void groupByExpression2() {
        List<QueryResult> results = executeQuery("groupby(payloadType, count(aggregateSequenceNumber))");
        results.forEach(System.out::println);
        assertEquals(5, results.size());
        // TODO: Validate result contents
    }
    @Test
    public void sort() {
        List<QueryResult> results = executeQuery("sortby(payloadType)");
        results.forEach(System.out::println);
        assertEquals(5, results.size());
        // TODO: Validate result contents
    }

    @Test
    public void testAverage() {
        List<QueryResult> results = executeQuery("groupBy(aggregateType, avg(aggregateSequenceNumber))");
        results.forEach(System.out::println);
    }

    @Test
    public void groupByExpression2WithFilter() {
        List<QueryResult> results = executeQuery("groupby(payloadType, count(aggregateSequenceNumber), max(aggregateSequenceNumber)) | count > 2 ");
        results.forEach(System.out::println);
        assertEquals(2, results.size());
        // TODO: Validate result contents
    }

    @Test
    public void containsExpression() {
        List<QueryResult> results = executeQuery("sortBy(aggregateType) | contains(aggregateType, aggregateSequenceNumber)");
        results.forEach(System.out::println);
        assertEquals(2, results.size());
        // TODO: Validate result contents
    }

    @Test
    public void equalExpression() {
        List<QueryResult> results = executeQuery("aggregateSequenceNumber = 1");
        results.forEach(System.out::println);
        assertEquals(1, results.size());
        // TODO: Validate result contents
    }

    @Test
    public void count() {
        List<QueryResult> results = executeQuery("count( )");
        results.forEach(System.out::println);
        assertEquals(5, results.size());
        // TODO: Validate result contents
    }
    @Test
    public void lastDay() {
        List<QueryResult> results = executeQuery("last day");
        results.forEach(System.out::println);
        assertEquals(5, results.size());
        // TODO: Validate result contents
    }

    @Test
    public void countWithExpression() {
        List<QueryResult> results = executeQuery("count(aggregateSequenceNumber)");
        results.forEach(System.out::println);
        assertEquals(5, results.size());
        // TODO: Validate result contents
    }

    @Test
    public void filterOnMetaData() {
        List<QueryResult> results = executeQuery("metaData(\"meta-key\") = \"text-value\" | metaData.otherkey = 42");
        assertEquals(1, results.size());
    }

    private List<QueryResult> executeQuery(String queryString) {
        Query query;
        try {
            query = new EventStoreQueryParser().parse(queryString);
        } catch (ParseException e) {
            throw new RuntimeException(e);
        }

        List<QueryResult> results = new ArrayList<>();
        Pipeline pipeline = new QueryProcessor().buildPipeline(query, e -> {
            results.add(e);
            return true;
        });
        System.out.println( pipeline.columnNames(EventExpressionResult.COLUMN_NAMES));
        eventStream.forEach(e -> pipeline.process(new DefaultQueryResult(new EventExpressionResult(e))));
        return results;
    }

}
