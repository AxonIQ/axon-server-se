package io.axoniq.axonserver.localstorage.query.expressions.functions;

import io.axoniq.axonserver.localstorage.query.Expression;
import io.axoniq.axonserver.localstorage.query.ExpressionContext;
import io.axoniq.axonserver.localstorage.query.ExpressionResult;
import io.axoniq.axonserver.localstorage.query.result.ListExpressionResult;
import io.axoniq.axonserver.localstorage.query.result.NullExpressionResult;
import io.axoniq.axonserver.localstorage.query.result.NumericExpressionResult;
import io.axoniq.axonserver.localstorage.query.result.StringExpressionResult;
import io.axoniq.axonserver.localstorage.query.result.XmlExpressionResult;
import org.w3c.dom.Element;
import org.w3c.dom.NodeList;
import org.w3c.dom.Text;
import org.xml.sax.InputSource;

import java.util.ArrayList;
import java.util.List;
import javax.xml.namespace.QName;
import javax.xml.xpath.XPath;
import javax.xml.xpath.XPathConstants;
import javax.xml.xpath.XPathExpressionException;
import javax.xml.xpath.XPathFactory;

/**
 * Author: marc
 * xpath(element, xpath [,resultType])
 */
public class XPathExpression implements Expression {
    private final String alias;
    private final Expression document;
    private final Expression xpath;
    private static final XPathFactory xpathFactory = XPathFactory.newInstance();
    private final Expression outputType;

    public XPathExpression(String alias, Expression[] expressions) {
        this.alias = alias;
        this.document = expressions[0];
        this.xpath = expressions[1];
        if( expressions.length > 2) {
            this.outputType = expressions[2];
        } else {
            this.outputType = null;
        }
    }

    @Override
    public ExpressionResult apply(ExpressionContext expressionContext, ExpressionResult input) {
        Object doc = document.apply(expressionContext, input).asXml();
        String xpathValue = xpath.apply(expressionContext, input).getValue().toString();
        XPath xpathEvaluator = xpathFactory.newXPath();
        Object result = null;
        ExpressionResult outputTypeValue = null;
        try {
            if( outputType != null) {
                outputTypeValue = outputType.apply(expressionContext, input);
                if( doc instanceof InputSource) {
                    result = xpathEvaluator.compile(xpathValue).evaluate(
                            (InputSource)doc, outputType(outputTypeValue));
                } else {
                    result = xpathEvaluator.compile(xpathValue).evaluate(
                            doc, outputType(outputTypeValue));
                }
            } else {
                if( doc instanceof InputSource) {
                    result = xpathEvaluator.compile(xpathValue).evaluate((InputSource) doc );
                } else {
                    result = xpathEvaluator.compile(xpathValue).evaluate(doc);
                }
            }
        } catch (XPathExpressionException e) {
            throw new RuntimeException(e.getMessage(), e);
        }
        List<ExpressionResult> values = new ArrayList<>();
        if( result instanceof Element) {
            return toExpressionResult(result, outputTypeValue);
        }

        if( result instanceof NodeList) {
            NodeList resultArray = (NodeList)result;
            for( int i = 0; i < resultArray.getLength(); i++) {
                values.add(toExpressionResult(resultArray.item(i), outputTypeValue));
            }
            return new ListExpressionResult(values);
        }

        return toExpressionResult(result, outputTypeValue);
    }

    private QName outputType(ExpressionResult outputTypeValue) {
        if( outputTypeValue == null || ! outputTypeValue.isNonNull()) return XPathConstants.NODESET;

        switch (outputTypeValue.toString().toUpperCase()) {
            case "NODESET":
                return XPathConstants.NODESET;
            case "NODE":
                return XPathConstants.NODE;
            case "STRING":
                return XPathConstants.STRING;
            case "NUMBER":
                return XPathConstants.NUMBER;
            default:
        }

        return XPathConstants.NODESET;
    }

    private ExpressionResult toExpressionResult(Object value, ExpressionResult outputTypeValue) {
        if( value instanceof String) {
            if( outputTypeValue != null && "NUMBER".equalsIgnoreCase(outputTypeValue.toString())) {
                return new NumericExpressionResult((String)value);
            }
            return new StringExpressionResult((String)value);
        }

        if( value instanceof Number) return new NumericExpressionResult(((Number)value).doubleValue());
        if( value instanceof Text) return new StringExpressionResult(((Text)value).getTextContent());
        if( value instanceof Element) return new XmlExpressionResult(((Element)value));
        return NullExpressionResult.INSTANCE;
    }

    @Override
    public String alias() {
        return alias;
    }
}
