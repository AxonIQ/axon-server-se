package io.axoniq.axonserver.applicationevents;

import org.junit.*;
import org.junit.runner.*;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.mock.mockito.MockBean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.event.EventListener;
import org.springframework.test.context.junit4.SpringRunner;

import static org.mockito.Mockito.*;

/**
 * Integration tests for {@link LocalEventPublisher}
 *
 * @author Sara Pellegrini
 */
@RunWith(SpringRunner.class)
@SpringBootTest(classes = LocalEventPublisher.class)
@Configuration
public class LocalEventPublisherTest {

    @Autowired
    private LocalEventPublisher testSubject;
    @MockBean
    private Listener listener;

    @Test
    public void publishEvent() {
        Object event = new Object();
        verify(listener, times(0)).on(event);
        testSubject.publishEvent(event);
        verify(listener, times(1)).on(event);
    }

    private static class Listener {

        @EventListener
        public void on(Object object) {
        }
    }
}