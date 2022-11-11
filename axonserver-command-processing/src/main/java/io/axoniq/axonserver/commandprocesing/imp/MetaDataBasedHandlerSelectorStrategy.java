package io.axoniq.axonserver.commandprocesing.imp;

import io.axoniq.axonserver.commandprocessing.spi.Command;
import io.axoniq.axonserver.commandprocessing.spi.CommandHandler;
import io.axoniq.axonserver.commandprocessing.spi.CommandHandlerSubscription;
import io.axoniq.axonserver.commandprocessing.spi.Metadata;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Serializable;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.stream.StreamSupport;

public class MetaDataBasedHandlerSelectorStrategy implements HandlerSelectorStrategy {

    private static final Logger logger = LoggerFactory.getLogger(MetaDataBasedHandlerSelectorStrategy.class);

    @Override
    public Set<CommandHandlerSubscription> select(Set<CommandHandlerSubscription> candidates, Command command) {
        if (logger.isDebugEnabled()) {
            logger.debug("{}[{}] Selecting based on metadata", command.commandName(),
                         command.context());
        }

        Metadata metadata = command.metadata();
        Map<CommandHandlerSubscription, Integer> scorePerClient = new HashMap<>();
        candidates.forEach(candidate -> scorePerClient.computeIfAbsent(candidate,
                                                                       m -> score(metadata, m.commandHandler())));

        return getHighestScore(scorePerClient);
    }

    private Integer score(Metadata metaDataMap, CommandHandler client) {
        Metadata clientTags = client.metadata();

        return StreamSupport.stream(metaDataMap.metadataKeys().spliterator(), false)
                            .filter(k -> !Metadata.isInternal(k))
                            .reduce(0,
                                    (score, key) -> score + match(metaDataMap.metadataValue(key, null),
                                                                  clientTags.metadataValue(key, null)), Integer::sum);
    }

    private int match(Serializable requestValue, Serializable handlerValue) {
        return requestValue == null || handlerValue == null ? 0 : matchValues(requestValue, handlerValue);
    }

    private int matchValues(Serializable value, Serializable metaDataValue) {
        return String.valueOf(value).equals(String.valueOf(metaDataValue)) ? 1 : -1;
    }

    private Set<CommandHandlerSubscription> getHighestScore(Map<CommandHandlerSubscription, Integer> scorePerClient) {
        Set<CommandHandlerSubscription> bestClients = new HashSet<>();
        int highest = Integer.MIN_VALUE;
        for (Map.Entry<CommandHandlerSubscription, Integer> score : scorePerClient.entrySet()) {
            if (score.getValue() > highest) {
                bestClients.clear();
                highest = score.getValue();
            }
            if (score.getValue() == highest) {
                bestClients.add(score.getKey());
            }
        }
        return bestClients;
    }
}
