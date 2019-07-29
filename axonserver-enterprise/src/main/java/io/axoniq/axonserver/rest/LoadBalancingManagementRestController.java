package io.axoniq.axonserver.rest;

import io.axoniq.axonserver.component.processor.balancing.jpa.LoadBalancingStrategy;
import io.axoniq.axonserver.enterprise.cluster.RaftConfigServiceFactory;
import io.axoniq.axonserver.enterprise.component.processor.balancing.jpa.BaseProcessorLoadBalancing;
import io.axoniq.axonserver.enterprise.component.processor.balancing.stategy.LoadBalanceStrategyController;
import io.axoniq.axonserver.enterprise.component.processor.balancing.stategy.MergedProcessorLoadBalancingService;
import io.axoniq.axonserver.grpc.internal.ProcessorLBStrategy;
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

import static java.util.stream.Collectors.toMap;

/**
 * Rest APIs to manage Processor Load Balancing Strategies.
 *
 * @author Marc Gathier
 * @since 4.1
 */
@RestController
@Transactional
@RequestMapping("v1")
public class LoadBalancingManagementRestController {

    private final RaftConfigServiceFactory raftServiceFactory;

    private final MergedProcessorLoadBalancingService processorService;

    private final LoadBalanceStrategyController strategyController;


    public LoadBalancingManagementRestController(
            RaftConfigServiceFactory raftServiceFactory,
            MergedProcessorLoadBalancingService processorService,
            LoadBalanceStrategyController strategyController) {
        this.raftServiceFactory = raftServiceFactory;
        this.processorService = processorService;
        this.strategyController = strategyController;
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

    @PutMapping("components/{component}/processors/{processor}/loadbalance")
    @Deprecated
    public void setStrategy(@PathVariable("component") String component,
                            @PathVariable("processor") String processor,
                            @RequestParam("context") String context,
                            @RequestParam("strategy") String strategy) {
        setStrategy(processor, context, strategy);
    }

    /**
     * Updates the autoloadbalance stategy for a specific progressing group.
     *
     * @param processor the processing group
     * @param context   the context for the processing group
     * @param strategy  the loadbalance stategy to apply
     */
    @PutMapping("processors/{processor}/autoloadbalance")
    public void setStrategy(@PathVariable("processor") String processor,
                            @RequestParam("context") String context,
                            @RequestParam("strategy") String strategy){
        ProcessorLBStrategy update = ProcessorLBStrategy.newBuilder()
                                                        .setProcessor(processor)
                                                        .setContext(context)
                                                        .setStrategy(strategy)
                                                        .build();
        raftServiceFactory.getRaftConfigService().updateProcessorLoadBalancing(update);
    }

    @GetMapping("components/{component}/processors/loadbalance/strategies")
    @Deprecated
    public Map<String, String> getComponentStrategies(@PathVariable("component") String component,
                                                      @RequestParam("context") String context) {
        return getStrategies(context);
    }

    /**
     * Retrieves a list of auto-load-balance strategies defined for processing groups in this context.
     *
     * @param context the context
     * @return map of processing group/load balance strategy combinations
     */
    @GetMapping("processors/autoloadbalance/strategies")
    public Map<String, String> getStrategies(@RequestParam("context") String context) {
        return processorService.findByContext(context).stream()
                               .collect(toMap(o -> o.processor().name(), BaseProcessorLoadBalancing::strategy));
    }
}
