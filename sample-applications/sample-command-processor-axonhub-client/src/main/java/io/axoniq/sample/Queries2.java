package io.axoniq.sample;

import org.axonframework.queryhandling.QueryHandler;
import org.springframework.stereotype.Component;

/**
 * Author: marc
 */
@Component
public class Queries2 {

    @QueryHandler
    public String echo(String cmd) {
        return cmd + cmd;
    }

}
