package io.axoniq.axonserver.rest;

import io.axoniq.axonserver.component.processor.balancing.jpa.LoadBalancingStrategy;
import io.axoniq.axonserver.enterprise.replication.admin.RaftConfigServiceFactory;
import io.axoniq.axonserver.enterprise.component.processor.balancing.jpa.BaseProcessorLoadBalancing;
import io.axoniq.axonserver.enterprise.component.processor.balancing.stategy.LoadBalanceStrategyController;
import io.axoniq.axonserver.enterprise.component.processor.balancing.stategy.MergedProcessorLoadBalancingService;
import io.axoniq.axonserver.grpc.internal.ProcessorLBStrategy;
import io.axoniq.axonserver.logging.AuditLog;
import org.slf4j.Logger;
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

import java.security.Principal;
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

    private static final Logger auditLog = AuditLog.getLogger();

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
    public void saveStrategy(@RequestBody Map<String, String> body, Principal principal) {
        auditLog.info("[{}] Request to save load balance strategies {}.", AuditLog.username(principal), body);
        strategyController.save(new LoadBalancingStrategy(body));
    }

    @DeleteMapping("processors/loadbalance/strategies/{strategyName}")
    public void deleteStrategy(@PathVariable("strategyName") String strategyName, Principal principal) {
        auditLog.info("[{}] Request to delete load balance strategy {}.", AuditLog.username(principal), strategyName) ;
        strategyController.delete(strategyName);
    }

    @PatchMapping("processors/loadbalance/strategies/{strategyName}/factoryBean/{factoryBean}")
    public void updateFactoryBean(@PathVariable("strategyName") String strategyName,
                                  @PathVariable("factoryBean") String factoryBean, Principal principal) {
        auditLog.info("[{}] Request to update factory bean for {} to {}.",
                      AuditLog.username(principal),
                      strategyName,
                      factoryBean) ;
        strategyController.updateFactoryBean(strategyName, factoryBean);
    }

    @PatchMapping("processors/loadbalance/strategies/{strategyName}/label/{label}")
    public void updateLabel(@PathVariable("strategyName") String strategyName,
                            @PathVariable("label") String label, Principal principal) {
        auditLog.info("[{}] Request to update label for {} to {}.", AuditLog.username(principal), strategyName, label) ;
        strategyController.updateLabel(strategyName, label);
    }

    @PutMapping("components/{component}/processors/{processor}/loadbalance")
    @Deprecated
    public void setStrategy(@PathVariable("component") String component,
                            @PathVariable("processor") String processor,
                            @RequestParam("context") String context,
                            @RequestParam("strategy") String strategy, Principal principal) {
        setAutoLoadBalanceStrategy(processor, context, strategy, "", principal);
    }

    /**
     * Updates the autoloadbalance strategy for a specific progressing group.
     *
     * @param processor            the processing group
     * @param context              the context for the processing group
     * @param strategy             the loadbalance strategy to apply
     * @param tokenStoreIdentifier identifier of the token store for the processor
     */
    @PutMapping("processors/{processor}/autoloadbalance")
    public void setAutoLoadBalanceStrategy(@PathVariable("processor") String processor,
                                           @RequestParam("context") String context,
                                           @RequestParam("strategy") String strategy,
                                           @RequestParam("tokenStoreIdentifier") String tokenStoreIdentifier,
                                           Principal principal) {
        auditLog.info("[{}] Request to update strategy for processor {} in context {} to {}.",
                      AuditLog.username(principal), processor, context, strategy);
        ProcessorLBStrategy update = ProcessorLBStrategy.newBuilder()
                                                        .setProcessor(processor)
                                                        .setTokenStoreIdentifier(tokenStoreIdentifier)
                                                        .setContext(context)
                                                        .setStrategy(strategy)
                                                        .build();
        raftServiceFactory.getRaftConfigService().updateProcessorLoadBalancing(update);
    }

    @GetMapping("components/{component}/processors/loadbalance/strategies")
    @Deprecated
    public Map<String, String> getComponentStrategies(@PathVariable("component") String component,
                                                      @RequestParam("context") String context, Principal principal) {
        return getStrategies(context, principal);
    }

    /**
     * Retrieves a list of auto-load-balance strategies defined for processing groups in this context.
     *
     * @param context the context
     * @return map of processing group/load balance strategy combinations
     */
    @GetMapping("processors/autoloadbalance/strategies")
    public Map<String, String> getStrategies(@RequestParam("context") String context, Principal principal) {
        auditLog.info("[{}] Request to get strategies for context {}.", AuditLog.username(principal), context);
        return processorService.findByContext(context).stream().collect(
                toMap(o -> o.processor().fullName(), BaseProcessorLoadBalancing::strategy));
    }
}
