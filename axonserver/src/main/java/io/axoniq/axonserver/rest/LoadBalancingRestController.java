package io.axoniq.axonserver.rest;

import io.axoniq.axonserver.component.processor.balancing.LoadBalancingStrategy.Factory;
import io.axoniq.axonserver.component.processor.balancing.TrackingEventProcessor;
import io.axoniq.axonserver.component.processor.balancing.jpa.LoadBalanceStrategyController;
import io.axoniq.axonserver.component.processor.balancing.jpa.LoadBalancingStrategy;
import io.axoniq.axonserver.component.processor.balancing.jpa.ProcessorLoadBalancing;
import io.axoniq.axonserver.component.processor.balancing.jpa.ProcessorLoadBalancingController;
import io.axoniq.axonserver.component.processor.balancing.strategy.NoLoadBalanceStrategy;
import io.axoniq.axonserver.serializer.Printable;
import org.springframework.transaction.annotation.Transactional;
import org.springframework.web.bind.annotation.DeleteMapping;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PatchMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.PutMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

import java.util.Map;
import java.util.Set;

import static java.util.stream.Collectors.toMap;

/**
 * Created by Sara Pellegrini on 14/08/2018.
 * sara.pellegrini@gmail.com
 */
@RestController
@Transactional
@RequestMapping("v1")
public class LoadBalancingRestController {

    private final ProcessorLoadBalancingControllerFacade processorController;

    private final LoadBalanceStrategyControllerFacade strategyController;

    private final Map<String, Factory> factories;

    public LoadBalancingRestController(
            ProcessorLoadBalancingControllerFacade processorController,
            LoadBalanceStrategyControllerFacade strategyController,
            Map<String, Factory> factoryMap) {
        this.processorController = processorController;
        this.strategyController = strategyController;
        this.factories = factoryMap;
    }

    @GetMapping("processors/loadbalance/strategies")
    public Iterable<? extends Printable> getStrategies() {
        return strategyController.findAll();
    }

    @GetMapping("processors/loadbalance/strategies/factories")
    public Set<String> getLoadBalancingStrategyFactoryBean(){
        return factories.keySet();
    }

    @PostMapping("processors/loadbalance/strategies")
    public void saveStrategy(@RequestBody Map<String,String> body) {
        strategyController.save(new LoadBalancingStrategy(body));
    }

    @DeleteMapping("processors/loadbalance/strategies/{strategyName}")
    public void deleteStrategy(@PathVariable("strategyName") String strategyName){
        strategyController.delete(strategyName);
    }

    @PatchMapping("processors/loadbalance/strategies/{strategyName}/factoryBean/{factoryBean}")
    public void updateFactoryBean(@PathVariable("strategyName") String strategyName,
                                  @PathVariable("factoryBean") String factoryBean){
        strategyController.updateFactoryBean(strategyName,factoryBean);
    }

    @PatchMapping("processors/loadbalance/strategies/{strategyName}/label/{label}")
    public void updateLabel(@PathVariable("strategyName") String strategyName,
                            @PathVariable("label") String label){
        strategyController.updateLabel(strategyName,label);
    }

    @PatchMapping("components/{component}/processors/{processor}/loadbalance")
    public void loadBalance(@PathVariable("component") String component,
                            @PathVariable("processor") String processor,
                            @RequestParam("context") String context,
                            @RequestParam("strategy") String strategyName) {
        TrackingEventProcessor trackingProcessor = new TrackingEventProcessor(processor, component, context);
        LoadBalancingStrategy strategy = strategyController.findByName(strategyName);
        Factory factory = factories.getOrDefault(strategy.factoryBean(), NoLoadBalanceStrategy::new);
        factory.create().balance(trackingProcessor).perform();
    }

    @PutMapping("components/{component}/processors/{processor}/loadbalance")
    public void setStrategy(@PathVariable("component") String component,
                            @PathVariable("processor") String processor,
                            @RequestParam("context") String context,
                            @RequestParam("strategy") String strategy){
        TrackingEventProcessor trackingProcessor = new TrackingEventProcessor(processor, component, context);
        processorController.save(new ProcessorLoadBalancing(trackingProcessor, strategy));
    }

    @GetMapping("components/{component}/processors/loadbalance/strategies")
    public Map<String, String> getComponentStrategies(@PathVariable("component") String component,
                                                      @RequestParam("context") String context) {
        return processorController.findByComponentAndContext(component, context).stream()
                                  .collect(toMap(o -> o.processor().name(), ProcessorLoadBalancing::strategy));
    }

}
