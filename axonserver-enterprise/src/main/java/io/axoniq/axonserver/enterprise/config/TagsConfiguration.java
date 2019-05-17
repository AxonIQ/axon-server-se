package io.axoniq.axonserver.enterprise.config;

import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.context.annotation.Configuration;

import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.Map;

/**
 * Configuration property allowing users to define tags (or labels) for their Axon Server nodes. This allows, for example,
 * clients to make decisions about which node to connect to.
 *
 * @author Greg Woods
 * @since 4.2
 *
 */
@ConfigurationProperties("axoniq.axonserver")
@Configuration
public class TagsConfiguration {

    private Map<String,String> tags = new HashMap<>();

    public Map<String,String> getTags(){
        return tags;
    }

    public void setTags(Map<String,String> tags){
        this.tags=tags;
    }
}