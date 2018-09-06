package io.axoniq.axonhub.rest;

import io.axoniq.axonhub.component.ComponentItems;
import io.axoniq.axonhub.component.instance.Clients;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

/**
 * Created by Sara Pellegrini on 23/03/2018.
 * sara.pellegrini@gmail.com
 */
@RestController
@RequestMapping("v1/components")
public class ClientApplicationRestController {

    private final Clients clients;

    public ClientApplicationRestController(Clients clients) {
        this.clients = clients;
    }

    @GetMapping("{component}/instances")
    public Iterable getComponentInstances(@PathVariable("component") String component, @RequestParam("context") String context ){
        return new ComponentItems<>(component,context,clients);
    }

}
