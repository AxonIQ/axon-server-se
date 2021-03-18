/*
 * Copyright (c) 2017-2021 AxonIQ B.V. and/or licensed to AxonIQ B.V.
 * under one or more contributor license agreements.
 *
 *  Licensed under the AxonIQ Open Source License Agreement v1.0;
 *  you may not use this file except in compliance with the license.
 *
 */

package io.axoniq.axonserver.plugin;

import io.axoniq.axonserver.interceptor.PluginEnabledEvent;
import io.axoniq.axonserver.rest.PluginPropertyGroup;
import org.junit.*;
import org.osgi.framework.Bundle;

import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.junit.Assert.*;
import static org.mockito.Mockito.*;

/**
 * @author Marc Gathier
 */
public class PluginConfigurationManagerTest {

    private PluginConfigurationManager testSubject;
    private SampleConfigurationListener sampleConfigurationListener;

    @Before
    public void setUp() throws Exception {
        sampleConfigurationListener = new SampleConfigurationListener();
        OsgiController osgiController = mock(OsgiController.class);
        Bundle mockBundle = mock(Bundle.class);
        when(osgiController.getBundle(any())).thenReturn(mockBundle);
        when(osgiController.getConfigurationListeners(any())).thenReturn(Collections
                                                                                 .singleton(sampleConfigurationListener));
        testSubject = new PluginConfigurationManager(osgiController);
    }

    @Test
    public void onEnabled() {
        Map<String, Map<String, Object>> properties = new HashMap<>();
        properties.computeIfAbsent("demo", d -> new HashMap<>()).put("id1", "value1");
        testSubject.on(new PluginEnabledEvent("context", new PluginKey("ext", "version"), properties, true));
        Map<String, ?> config = sampleConfigurationListener.configurationPerContext.get("context");
        assertEquals(1, config.size());
    }

    @Test
    public void updateConfiguration() {
        Map<String, Map<String, Object>> properties = new HashMap<>();
        properties.computeIfAbsent("demo", d -> new HashMap<>()).put("id1", "value1");
        testSubject.updateConfiguration(new PluginKey("demo", "1.0"), "context", properties);
        Map<String, ?> config = sampleConfigurationListener.configurationPerContext.get("context");
        assertEquals(1, config.size());
    }

    @Test
    public void configuration() {
        List<PluginPropertyGroup> properties = testSubject.configuration(new PluginKey(
                "demoPlugin",
                "1.0"));
        assertEquals(1, properties.size());
        assertEquals(1, properties.get(0).getProperties().size());
    }

    private class SampleConfigurationListener implements ConfigurationListener {

        Map<String, Map<String, ?>> configurationPerContext = new HashMap<>();

        @Override
        public void removed(String context) {
            configurationPerContext.remove(context);
        }

        @Override
        public void updated(String context, Map<String, ?> configuration) {
            configurationPerContext.put(context, configuration);
        }

        @Override
        public Configuration configuration() {
            return new Configuration(Arrays.asList(
                    PluginPropertyDefinition.newBuilder("id1", "name1").build()
            ), "demo");
        }
    }
}