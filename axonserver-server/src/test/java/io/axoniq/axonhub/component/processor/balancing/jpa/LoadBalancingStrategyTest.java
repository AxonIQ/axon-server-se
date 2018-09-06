package io.axoniq.axonhub.component.processor.balancing.jpa;

import io.axoniq.axonhub.serializer.GsonMedia;
import io.axoniq.axonhub.serializer.Media;
import org.junit.*;

import static org.junit.Assert.*;

/**
 * Created by Sara Pellegrini on 20/08/2018.
 * sara.pellegrini@gmail.com
 */
public class LoadBalancingStrategyTest {

    @Test
    public void printTest() {
        LoadBalancingStrategy strategy = new LoadBalancingStrategy("myName", "myLabel", "myFactoryBean");
        Media media = new GsonMedia();
        strategy.printOn(media);
        assertEquals("{\"name\":\"myName\",\"label\":\"myLabel\",\"factoryBean\":\"myFactoryBean\"}",
                     media.toString());
    }
}