/*
 * Copyright (c) 2017-2019 AxonIQ B.V. and/or licensed to AxonIQ B.V.
 * under one or more contributor license agreements.
 *
 *  Licensed under the AxonIQ Open Source License Agreement v1.0;
 *  you may not use this file except in compliance with the license.
 *
 */

package io.axoniq.axonserver.localstorage.query.expressions.functions;

import io.axoniq.axonserver.localstorage.query.Expression;
import io.axoniq.axonserver.localstorage.query.ExpressionContext;
import io.axoniq.axonserver.localstorage.query.ExpressionResult;
import io.axoniq.axonserver.localstorage.query.expressions.Identifier;
import io.axoniq.axonserver.localstorage.query.result.ListExpressionResult;
import io.axoniq.axonserver.localstorage.query.result.NumericExpressionResult;
import io.axoniq.axonserver.localstorage.query.result.StringExpressionResult;
import io.axoniq.axonserver.localstorage.query.result.XmlExpressionResult;
import org.junit.*;

import static io.axoniq.axonserver.localstorage.query.expressions.ResultFactory.mapValue;
import static io.axoniq.axonserver.localstorage.query.expressions.ResultFactory.stringValue;
import static org.assertj.core.api.Assertions.assertThat;

/**
 * @author Marc Gathier
 */
public class XPathExpressionTest {
    private XPathExpression testSubject;
    private XPathExpression testSubjectWithoutType;
    private ExpressionContext expressionContext;
    private String xmlDocument = "<io.axoniq.axondb.performancetest.axonapp.DataAddedToDummyEvt><index>12692</index><id>beff70ef-3160-499b-8409-5bd5646f52f3</id><tracker>175143</tracker><test>" +
            "<data>First</data>" +
            "<data>Second</data>" +
            "<data>Third</data>" +
            "<data>Fourth</data>" +
            "</test></io.axoniq.axondb.performancetest.axonapp.DataAddedToDummyEvt>";

    @Before
    public void setUp()  {
        expressionContext = new ExpressionContext();
        testSubject = new XPathExpression("title", new Expression[]{
                new Identifier("test"),
                new Identifier("expression"),
                new Identifier("resultType")
        });
        testSubjectWithoutType = new XPathExpression("title", new Expression[]{
                new Identifier("test"),
                new Identifier("expression")
        });
    }

    @Test
    public void apply() {
        ExpressionResult result = testSubject.apply(expressionContext, mapValue("test", stringValue(xmlDocument),
                "expression", stringValue("//data/text()"), "resultType", stringValue("NODESET")));
        assertThat(result).isInstanceOf(ListExpressionResult.class);
        ListExpressionResult resultList = (ListExpressionResult) result;
        assertThat(resultList.getValue()).contains(new StringExpressionResult("Second"));
    }

    @Test
    public void applyWithNodes() {
        ExpressionResult result = testSubjectWithoutType.apply(expressionContext, mapValue("test", stringValue(xmlDocument),
                "expression", stringValue("count(//data)")));
        assertThat(result).isInstanceOf(StringExpressionResult.class);
        assertThat(result.toString()).isEqualTo("4");
    }
    @Test
    public void applyCount() {
        ExpressionResult result = testSubject.apply(expressionContext, mapValue("test", stringValue(xmlDocument),
                "expression", stringValue("count(//data)"), "resultType",stringValue("NUMBER")));
        assertThat(result).isInstanceOf(NumericExpressionResult.class);
        assertThat(result.getNumericValue().intValue()).isEqualTo(4);
    }

    @Test
    public void getIndex() {
        ExpressionResult result = testSubjectWithoutType.apply(expressionContext, mapValue("test", stringValue(xmlDocument),
                "expression", stringValue("//io.axoniq.axondb.performancetest.axonapp.DataAddedToDummyEvt/index")));
        assertThat(result).isInstanceOf(StringExpressionResult.class);
        assertThat(result.toString()).isEqualTo("12692");
    }

    @Test
    public void getIndexNode() {
        ExpressionResult result = testSubject.apply(expressionContext, mapValue("test", stringValue(xmlDocument),
                "expression", stringValue("//io.axoniq.axondb.performancetest.axonapp.DataAddedToDummyEvt/index"),
                "resultType", stringValue("node")));
        assertThat(result).isInstanceOf(XmlExpressionResult.class);
        assertThat(result.toString()).isEqualTo("<index>12692</index>");
    }

}
