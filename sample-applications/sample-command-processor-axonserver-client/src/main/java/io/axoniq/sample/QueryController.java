package io.axoniq.sample;

import org.axonframework.queryhandling.QueryGateway;
import org.axonframework.queryhandling.responsetypes.ResponseTypes;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import java.util.stream.Stream;

/**
 * Author: marc
 */
@RestController
@RequestMapping("/query")
public class QueryController {
    private final QueryGateway queryGateway;

    public QueryController(QueryGateway queryGateway) {
        this.queryGateway = queryGateway;
    }

    @GetMapping("/all")
    public List<Integer> echo(@RequestParam(value="text") String test) {
       Stream<Integer> resultStream = queryGateway.scatterGather(test, ResponseTypes.instanceOf(int.class), 20, TimeUnit.SECONDS);
        List l = resultStream.collect(Collectors.toList());
        System.out.println(l);
       return l;
    }
    @GetMapping("/all2")
    public List<String> echo2(@RequestParam(value="text") String test) {
        Stream<String> resultStream = queryGateway.send(test, String.class, 15, TimeUnit.SECONDS);
        return resultStream.collect(Collectors.toList());
    }
    @GetMapping("/one")
    public Future<Integer> echoOne(@RequestParam(value="text") String test) {
        return queryGateway.send(test, int.class);
    }

    @RequestMapping("batch")
    public Future<String> update(@RequestParam(value="count", defaultValue = "5")  int count) {
        CompletableFuture<String> result = new CompletableFuture<>();
        CountDownLatch resultCounter = new CountDownLatch(count);
        IntStream.range(0, count).forEach(i -> {
            queryGateway.send("test", int.class)
                    .whenComplete((r, t) -> {
                        resultCounter.countDown();
                        if( resultCounter.getCount() == 0) result.complete(count + " queries processed");
                    });
        });
        return result;
    }

}
