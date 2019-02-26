package net.axoniq.axonhub.custom;

import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

/**
 * @author Marc Gathier
 */
@RestController
public class SampleRestController {

    @GetMapping("/v1/public/sample")
    public String echo(@RequestParam("string") String string) {
        return string;
    }

}
