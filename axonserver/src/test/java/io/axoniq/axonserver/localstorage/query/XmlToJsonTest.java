/*
 * Copyright (c) 2017-2019 AxonIQ B.V. and/or licensed to AxonIQ B.V.
 * under one or more contributor license agreements.
 *
 *  Licensed under the AxonIQ Open Source License Agreement v1.0;
 *  you may not use this file except in compliance with the license.
 *
 */

package io.axoniq.axonserver.localstorage.query;


import org.json.JSONArray;
import org.json.JSONException;
import org.json.JSONObject;
import org.junit.*;
import org.xml.sax.Attributes;
import org.xml.sax.InputSource;
import org.xml.sax.helpers.DefaultHandler;

import java.io.StringReader;
import java.util.Stack;
import javax.xml.parsers.SAXParser;
import javax.xml.parsers.SAXParserFactory;

/**
 * @author Marc Gathier
 */
public class XmlToJsonTest {
    private String xmlDocument = "<io.axoniq.axondb.performancetest.axonapp.DataAddedToDummyEvt><index>12692</index><id>beff70ef-3160-499b-8409-5bd5646f52f3</id><tracker>175143</tracker><test>" +
            "<data>First</data>" +
            "<data>Second</data>" +
            "<data>Third</data>" +
            "<data>Fourth</data>" +
            "</test></io.axoniq.axondb.performancetest.axonapp.DataAddedToDummyEvt>";


    @Test
    public void transformXml() throws Exception {
        SAXParserFactory factory = SAXParserFactory.newInstance();
        factory.setNamespaceAware(true);
        SAXParser saxParser = factory.newSAXParser();
        Stack<JSONObject> currentJsonPath = new Stack<>();
        currentJsonPath.push(new JSONObject());
        DefaultHandler handler = new DefaultHandler() {

            String currentValue;


            @Override
            public void startElement(String uri, String localName, String qName, Attributes attributes) {
                JSONObject child = new JSONObject();


                for( int i = 0 ; i < attributes.getLength(); i++) {
                    put( child, "@" + attributes.getLocalName(i), attributes.getValue(i));
                }
                currentJsonPath.push(child);
            }

            private void put(JSONObject node, String name, Object value) {
                Object currentValue = null;
                try {
                    currentValue = node.get(name);
                } catch( JSONException jsonException) {
                }
                try {
                    if( currentValue instanceof JSONArray) {
                        ((JSONArray) currentValue).put(value);
                    } else if ( currentValue instanceof JSONObject) {
                        JSONArray array = new JSONArray();
                        array.put(currentValue);
                        array.put(value);
                        node.put(name, array);
                    } else {
                        node.put(name, value);
                    }
                } catch (JSONException e) {
                    throw new RuntimeException(e);
                }
            }
            @Override
            public void endElement(String uri, String localName, String qName) {
                JSONObject node = currentJsonPath.pop();

                if( !currentJsonPath.empty()) {
                    JSONObject parent = currentJsonPath.peek();
                    if( currentValue != null) {
                        put(parent, localName, currentValue);
                    } else {
                        put(parent, localName, node);
                    }
                }
                currentValue = null;

            }

            @Override
            public void characters(char[] ch, int start, int length) {
                currentValue = new String(ch, start, length).trim();
                if( currentValue.isEmpty()) currentValue = null;
            }

            @Override
            public void ignorableWhitespace(char[] ch, int start, int length) {
            }
        };

        saxParser.parse(new InputSource(new StringReader(xmlDocument)), handler);
        JSONObject jsonObject = new JSONObject();
        jsonObject.put("Test", 1);


        System.out.println(jsonObject);
        System.out.println(currentJsonPath.pop());
    }
}
