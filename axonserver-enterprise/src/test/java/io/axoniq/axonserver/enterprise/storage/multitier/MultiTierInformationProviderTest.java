package io.axoniq.axonserver.enterprise.storage.multitier;

import io.axoniq.axonserver.enterprise.replication.RaftGroupRepositoryManager;
import io.axoniq.axonserver.metric.BaseMetricName;
import io.axoniq.axonserver.metric.MeterFactory;
import io.micrometer.core.instrument.Tags;
import org.junit.*;

import java.util.Arrays;
import java.util.HashSet;
import java.util.Set;

import static org.junit.Assert.*;

/**
 * @author Marc Gathier
 */
public class MultiTierInformationProviderTest {

    private MultiTierInformationProvider testSubject;

    @Before
    public void setUp() {
        RaftGroupRepositoryManager raftGroupRepositoryManager = new RaftGroupRepositoryManager(null,
                                                                                               null,
                                                                                               null,
                                                                                               null,
                                                                                               null) {
            @Override
            public boolean hasLowerTier(String replicationGroup) {
                return true;
            }

            @Override
            public Set<String> nextTierEventStores(String replicationGroup) {
                if ("CONTEXT".equals(replicationGroup)) {
                    new HashSet<>(Arrays.asList("node-1", "node-2"));
                }
                return new HashSet<>(Arrays.asList("node-1", "node-2", "node-3"));
            }
        };

        FakeMetricCollector metricCollector = new FakeMetricCollector();
        metricCollector.gauge(BaseMetricName.AXON_EVENT_LAST_TOKEN.metric(),
                              Tags.of(MeterFactory.CONTEXT, "DEMO", "axonserver", "node-1"),
                              15);
        metricCollector.gauge(BaseMetricName.AXON_EVENT_LAST_TOKEN.metric(),
                              Tags.of(MeterFactory.CONTEXT, "DEMO", "axonserver", "node-2"),
                              10);
        metricCollector.gauge(BaseMetricName.AXON_EVENT_LAST_TOKEN.metric(),
                              Tags.of(MeterFactory.CONTEXT, "DEMO", "axonserver", "node-3"),
                              20);
        metricCollector.gauge(BaseMetricName.AXON_EVENT_LAST_TOKEN.metric(),
                              Tags.of(MeterFactory.CONTEXT, "CONTEXT", "axonserver", "node-1"),
                              1);



        testSubject = new MultiTierInformationProvider(raftGroupRepositoryManager, metricCollector);
    }

    @Test
    public void safeToken() {
        assertEquals(10, testSubject.safeToken("DEMO", BaseMetricName.AXON_EVENT_LAST_TOKEN));
    }

    @Test
    public void safeTokenMissingNode() {
        assertEquals(0, testSubject.safeToken("CONTEXT", BaseMetricName.AXON_EVENT_LAST_TOKEN));
    }
}