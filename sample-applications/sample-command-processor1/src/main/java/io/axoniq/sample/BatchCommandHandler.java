package io.axoniq.sample;

import org.axonframework.commandhandling.CommandHandler;
import org.springframework.stereotype.Controller;

/**
 * Author: marc
 */
@Controller
public class BatchCommandHandler {


    @CommandHandler
    public void handle(BatchCommand command) {
    }


}
