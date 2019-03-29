/*
 * Copyright (c) 2017-2019 AxonIQ B.V. and/or licensed to AxonIQ B.V.
 * under one or more contributor license agreements.
 *
 *  Licensed under the AxonIQ Open Source License Agreement v1.0;
 *  you may not use this file except in compliance with the license.
 *
 */

package io.axoniq.axonserver.localstorage.query.expressions.functions;

import io.axoniq.axonserver.queryparser.QueryElement;
import io.axoniq.axonserver.localstorage.query.Expression;
import io.axoniq.axonserver.localstorage.query.ExpressionRegistry;
import io.axoniq.axonserver.localstorage.query.PipeExpression;
import io.axoniq.axonserver.localstorage.query.expressions.AbstractExpressionFactory;
import io.axoniq.axonserver.localstorage.query.expressions.AndExpression;
import io.axoniq.axonserver.localstorage.query.expressions.Identifier;
import io.axoniq.axonserver.localstorage.query.expressions.ListExpression;
import io.axoniq.axonserver.localstorage.query.expressions.NotExpression;
import io.axoniq.axonserver.localstorage.query.expressions.NumericLiteral;
import io.axoniq.axonserver.localstorage.query.expressions.OrExpression;
import io.axoniq.axonserver.localstorage.query.expressions.SortExpression;
import io.axoniq.axonserver.localstorage.query.expressions.StringLiteral;

import java.util.Optional;

public class FunctionExpressionFactory extends AbstractExpressionFactory {

    private static final String COUNT = "count";
    private static final String MAX = "max";
    private static final String AVG = "avg";
    private static final String MIN = "min";
    private static final String LIST = "list";
    private static final String CONTAINS = "contains";
    private static final String JSONPATH = "jsonpath";
    private static final String SUBSTRING = "substring";
    private static final String MATCH = "match";
    private static final String FORMATDATE = "formatdate";
    private static final String MONTH = "month";
    private static final String DAY = "day";
    private static final String WEEK = "week";
    private static final String YEAR = "year";
    private static final String HOUR = "hour";
    private static final String METADATA = "metadata";
    public static final String VALUE = "value";

    @Override
    public Optional<Expression> buildExpression(QueryElement element, ExpressionRegistry registry) {
        switch (element.operator().toLowerCase()) {
            case COUNT:
                return Optional.of(new CountExpression(element.alias().orElse(COUNT),
                                                       optionalParameter(0, element.getParameters(), registry)
                                                               .orElse(null)));
            case MAX:
                return Optional.of(new MaxExpression(element.alias().orElse(MAX),
                                                     parameter(element.operator(), 0, element.getParameters(), registry)));
            case AVG:
                return Optional.of(new AvgExpression(element.alias().orElse(AVG),
                                                     parameter(element.operator(), 0, element.getParameters(), registry)));
            case MIN:
                return Optional.of(new MinExpression(element.alias().orElse(MIN),
                                                     parameter(element.operator(), 0, element.getParameters(), registry)));
            case LIST:
                return Optional.of(new ListExpression(element.alias().orElse(VALUE),
                                                      buildParameters(element.getParameters(), registry)));
            case "string-literal":
                return Optional.of(new StringLiteral(element.getLiteral()));
            case "numeric-literal":
                return Optional.of(new NumericLiteral(element.alias().orElse(VALUE), element.getLiteral()));
            case "identifier":
                return Optional.of(new Identifier(element.alias().orElse(element.getLiteral()), element.getLiteral()));
            case CONTAINS:
                return Optional.of(new ContainsExpression(element.alias().orElse(CONTAINS),
                                                          buildParameters(element.getParameters(), registry)));
            case JSONPATH:
                return Optional.of(new JsonPathExpression(element.alias().orElse(JSONPATH),
                        buildParameters(element.getParameters(), registry)));
            case "xpath":
                return Optional.of(new XPathExpression(element.alias().orElse("xpath"),
                        buildParameters(element.getParameters(), registry)));
            case "upper":
                return Optional.of(new UpperExpression(element.alias().orElse("upper"),
                        optionalParameter(0, element.getParameters(), registry)
                                .orElse(null)));
            case "lower":
                return Optional.of(new LowerExpression(element.alias().orElse("lower"),
                        optionalParameter(0, element.getParameters(), registry)
                                .orElse(null)));
            case "length":
                return Optional.of(new LengthExpression(element.alias().orElse("length"),
                        optionalParameter(0, element.getParameters(), registry)
                                .orElse(null)));
            case "or":
                return Optional.of(new OrExpression(element.alias().orElse(VALUE), buildParameters(element.getParameters(), registry)));
            case "and":
                return Optional.of(new AndExpression(element.alias().orElse(VALUE), buildParameters(element.getParameters(), registry)));
            case "not":
                return Optional.of(new NotExpression(element.alias().orElse(VALUE), buildParameters(element.getParameters(), registry)));
            case SUBSTRING:
                return Optional.of(new SubstringExpression(element.alias().orElse(SUBSTRING), buildParameters(element.getParameters(), registry)));
            case "concat":
                return Optional.of(new ConcatExpression(element.alias().orElse("concat"), buildParameters(element.getParameters(), registry)));
            case "left":
                return Optional.of(new LeftExpression(element.alias().orElse("left"), buildParameters(element.getParameters(), registry)));
            case "right":
                return Optional.of(new RightExpression(element.alias().orElse("right"), buildParameters(element.getParameters(), registry)));
            case MATCH:
                return Optional.of(new MatchExpression(element.alias().orElse(MATCH), buildParameters(element.getParameters(), registry)));
            case FORMATDATE:
                return Optional.of(new FormatDateExpression(element.alias().orElse(FORMATDATE), buildParameters(element.getParameters(), registry)));
            case DAY:
                return Optional.of(new FormatDateExpression(element.alias().orElse(DAY), buildParameters(element.getParameters(), registry)[0], new StringLiteral("F")));
            case WEEK:
                return Optional.of(new FormatDateExpression(element.alias().orElse(WEEK), buildParameters(element.getParameters(), registry)[0], new StringLiteral("w")));
            case MONTH:
                return Optional.of(new FormatDateExpression(element.alias().orElse(MONTH), buildParameters(element.getParameters(), registry)[0], new StringLiteral("MM")));
            case YEAR:
                return Optional.of(new FormatDateExpression(element.alias().orElse(YEAR), buildParameters(element.getParameters(), registry)[0], new StringLiteral("yyyy")));
            case HOUR:
                return Optional.of(new FormatDateExpression(element.alias().orElse(HOUR), buildParameters(element.getParameters(), registry)[0], new StringLiteral("HH")));
            case METADATA:
                return Optional.of(new MetaDataExpression(element.alias().orElse(null), buildParameters(element.getParameters(), registry)));
            default:
        }
        return Optional.empty();
    }

    @Override
    public Optional<PipeExpression> buildPipeExpression(QueryElement element, ExpressionRegistry registry) {
        switch (element.operator().toLowerCase()) {
            case "select":
                return Optional.of(new SelectExpression(buildParameters(element.getParameters(), registry)));
            case "groupby":
                return Optional.of(new GroupByExpression(buildParameters(element.getParameters(), registry)));
            case "limit":
                return Optional.of(new LimitExpression(optionalParameter(0, element.getParameters(), registry).orElse(new NumericLiteral("1000", "1000"))));
            case "sortby":
                return Optional.of(new SortExpression(buildParameters(element.getParameters(), registry)));
            case "or":
                return Optional.of(new OrExpression(element.alias().orElse(VALUE), buildParameters(element.getParameters(), registry)));
            case "and":
                return Optional.of(new AndExpression(element.alias().orElse(VALUE), buildParameters(element.getParameters(), registry)));
            case COUNT:
                return Optional.of(new CountExpression(element.alias().orElse(COUNT),
                                                       optionalParameter(0, element.getParameters(), registry)
                                                               .orElse(null)));
            case CONTAINS:
                return Optional.of(new ContainsExpression(element.alias().orElse(CONTAINS),
                                                          buildParameters(element.getParameters(), registry)));
            case MATCH:
                return Optional.of(new MatchExpression(element.alias().orElse(MATCH),
                        buildParameters(element.getParameters(), registry)));
            case "not":
                return Optional.of(new NotExpression(element.alias().orElse(VALUE), buildParameters(element.getParameters(), registry)));
            case AVG:
                return Optional.of(new AvgExpression(element.alias().orElse(AVG), parameter(element.operator(), 0, element.getParameters(), registry)));
            case MAX :
                return Optional.of(new MaxExpression(element.alias().orElse(MAX), parameter(element.operator(), 0, element.getParameters(), registry)));
            case MIN:
                return Optional.of(new MinExpression(element.alias().orElse(MIN), parameter(element.operator(), 0, element.getParameters(), registry)));
            case SUBSTRING:
                return Optional.of(new SubstringExpression(element.alias().orElse(SUBSTRING), buildParameters(element.getParameters(), registry)));
            case FORMATDATE:
                return Optional.of(new FormatDateExpression(element.alias().orElse(FORMATDATE), buildParameters(element.getParameters(), registry)));
            case DAY:
                return Optional.of(new FormatDateExpression(element.alias().orElse(DAY), buildParameters(element.getParameters(), registry)[0], new StringLiteral("F")));
            case WEEK:
                return Optional.of(new FormatDateExpression(element.alias().orElse(WEEK), buildParameters(element.getParameters(), registry)[0], new StringLiteral("w")));
            case MONTH:
                return Optional.of(new FormatDateExpression(element.alias().orElse(MONTH), buildParameters(element.getParameters(), registry)[0], new StringLiteral("MM")));
            case YEAR:
                return Optional.of(new FormatDateExpression(element.alias().orElse(YEAR), buildParameters(element.getParameters(), registry)[0], new StringLiteral("yyyy")));
            case HOUR:
                return Optional.of(new FormatDateExpression(element.alias().orElse(HOUR), buildParameters(element.getParameters(), registry)[0], new StringLiteral("HH")));
            default:
        }
        return Optional.empty();
    }
}
