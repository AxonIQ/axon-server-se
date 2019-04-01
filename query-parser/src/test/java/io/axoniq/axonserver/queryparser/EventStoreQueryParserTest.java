/*
 * Copyright (c) 2017-2019 AxonIQ B.V. and/or licensed to AxonIQ B.V.
 * under one or more contributor license agreements.
 *
 *  Licensed under the AxonIQ Open Source License Agreement v1.0;
 *  you may not use this file except in compliance with the license.
 *
 */

package io.axoniq.axonserver.queryparser;

import org.junit.Test;

/**
 * Author: marc
 */
public class EventStoreQueryParserTest {
    @Test
    public void parseInfixFunction() throws Exception {
        Query parsedQuery = new EventStoreQueryParser().parse("payloadData contains \"marc\"");
        System.out.println( parsedQuery);
        //assertEquals(1, parsedQuery.children.size());
    }
    @Test
    public void parseSimpleFunction() throws Exception {
        Query parsedQuery = new EventStoreQueryParser().parse("contains(payloadData, \"marc\")");
        System.out.println( parsedQuery);
        //assertEquals(1, parsedQuery.children.size());
    }
    @Test
    public void parseEscapedQuote() throws Exception {
        Query parsedQuery = new EventStoreQueryParser().parse("payloadData contains \"\\\"marc\\\"\"");
        System.out.println( parsedQuery);
        // assertEquals(1, parsedQuery.children.size());
    }

    @Test
    public void parseTimeConstraint() throws Exception {
        Query parsedQuery = new EventStoreQueryParser().parse("last 2.5 secs");
        System.out.println( parsedQuery);
        // System.out.println( parsedQuery.getStartTime());
        //assertEquals(1, parsedQuery.children.size());
    }
    @Test
    public void parseConditionAndTimeConstraint() throws Exception {
        Query parsedQuery = new EventStoreQueryParser().parse("a>b|c>d|last sec");
        System.out.println( parsedQuery);
//        System.out.println( parsedQuery.getStartTime());
//        assertEquals(3, parsedQuery.children.size());
    }

    @Test
    public void parseTimeConstraintFirst() throws Exception {
        Query parsedQuery = new EventStoreQueryParser().parse("last 2.5 days | aggregateSequenceNumber > 150");
        System.out.println( parsedQuery);
//        System.out.println( parsedQuery.getStartTime());
//        assertEquals(2, parsedQuery.children.size());
    }

    @Test
    public void parseOneMore() throws Exception{
        Query parsedQuery = new EventStoreQueryParser().parse("groupby( payloadType, count( aggregateSequenceNumber) as count, max( aggregateSequenceNumber) as max) | count > 2");
        System.out.println( parsedQuery);
    }
    @Test
    public void parseExprList() throws Exception{
        Query parsedQuery = new EventStoreQueryParser().parse("groupby( [payloadType+10, aggregateType], count( aggregateSequenceNumber) as count, max( aggregateSequenceNumber) as max)");
        System.out.println( parsedQuery);
    }

    @Test
    public void parseArith() throws Exception{
        Query parsedQuery = new EventStoreQueryParser().parse("aggregateSequenceNumber + 10 > 100");
        System.out.println( parsedQuery);
    }

    @Test
    public void parseAlias() throws Exception{
        Query parsedQuery = new EventStoreQueryParser().parse("aggregateSequenceNumber + 10 > 100 as high");
        System.out.println( parsedQuery);
    }

    @Test
    public void parsePrecedence() throws Exception{
        Query parsedQuery = new EventStoreQueryParser().parse("(aggregateSequenceNumber + 10) * 10 > 100");
        System.out.println( parsedQuery);
    }

    @Test
    public void parsePrefixNotation() throws Exception{
        Query parsedQuery = new EventStoreQueryParser().parse(">(*(+(aggregateSequenceNumber, 10), 10),100)");
        System.out.println( parsedQuery);
    }

    @Test
    public void parseOr() throws Exception{
        Query parsedQuery = new EventStoreQueryParser().parse("aggregateSequenceNumber > 10 OR aggregateSequenceNumber < 0");
        System.out.println( parsedQuery);
    }
    @Test
    public void parseAnd() throws Exception{
        Query parsedQuery = new EventStoreQueryParser().parse("aggregateSequenceNumber > 10 AND (aggregateSequenceNumber < 0 or aggregateId = 0)");
        System.out.println( parsedQuery);
    }
    @Test
    public void parseNot() throws Exception{
        Query parsedQuery = new EventStoreQueryParser().parse("NOT aggregateId contains 123");
        System.out.println( parsedQuery);
    }

    @Test
    public void parseMin() throws Exception{
        Query parsedQuery = new EventStoreQueryParser().parse("min(timestamp) as \"min\"");
        System.out.println( parsedQuery);
    }

}