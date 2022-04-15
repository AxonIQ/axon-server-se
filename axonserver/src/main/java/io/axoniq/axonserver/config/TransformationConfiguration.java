package io.axoniq.axonserver.config;

import io.axoniq.axonserver.eventstore.transformation.api.EventStoreTransformationService;
import io.axoniq.axonserver.eventstore.transformation.requestprocessor.DefaultEventStoreTransformationService;
import io.axoniq.axonserver.eventstore.transformation.requestprocessor.EventStoreTransformationRepository;
import io.axoniq.axonserver.eventstore.transformation.requestprocessor.LocalTransformers;
import io.axoniq.axonserver.eventstore.transformation.requestprocessor.Transformers;
import io.axoniq.axonserver.localstorage.AutoCloseableEventProvider;
import io.axoniq.axonserver.localstorage.ContextEventIteratorFactory;
import io.axoniq.axonserver.localstorage.LocalEventStore;
import io.axoniq.axonserver.localstorage.LocalEventStoreInitializationObservable;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.autoconfigure.condition.ConditionalOnMissingBean;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

@Configuration
public class TransformationConfiguration {

    @Bean
    public ContextEventIteratorFactory eventProviderFactory(LocalEventStore eventStore) {
        return context -> {
            AutoCloseableEventProvider autoCloseableEventProvider =
                    new AutoCloseableEventProvider(token -> eventStore.eventIterator(context, token));
            return autoCloseableEventProvider::event;
        };
    }

    @Bean
    public Transformers transformers(EventStoreTransformationRepository repository, ContextEventIteratorFactory iteratorFactory){
        return new LocalTransformers(iteratorFactory::createFrom, repository);
    }

    @Bean
    @ConditionalOnMissingBean(EventStoreTransformationService.class)
    public EventStoreTransformationService eventStoreTransformationService(Transformers transformers) {
        return new DefaultEventStoreTransformationService(transformers);
    }

    @Autowired
    public void configure(LocalEventStoreInitializationObservable initObservable, Transformers transformers) {
        initObservable.accept(context -> transformers.transformerFor(context)
                                                     .restart()
                                                     .subscribe());
    }


}
