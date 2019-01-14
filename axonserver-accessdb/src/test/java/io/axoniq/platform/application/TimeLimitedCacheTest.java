package io.axoniq.platform.application;

import org.junit.Assert;
import org.junit.Test;

/**
 * @author Marc Gathier
 */
public class TimeLimitedCacheTest {
    @Test
    public void put() throws Exception {
        TimeLimitedCache<String, String> cache = new TimeLimitedCache<>(100);
        cache.put("Test", "Test123");
        Assert.assertEquals(1, cache.entries.size());
    }

    @Test
    public void get() throws Exception {
        TimeLimitedCache<String, String> cache = new TimeLimitedCache<>(10);
        cache.put("Test", "Test123");
        Assert.assertNotNull(cache.get("Test"));
        Thread.sleep(11);
        Assert.assertNull(cache.get("Test"));
    }

}
