package io.axoniq.axonhub.component.processor.balancing.jpa;

import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.data.jpa.repository.Modifying;
import org.springframework.data.jpa.repository.Query;

/**
 * Created by Sara Pellegrini on 14/08/2018.
 * sara.pellegrini@gmail.com
 */
public interface LoadBalanceStrategyRepository extends JpaRepository<LoadBalancingStrategy, Integer> {

    LoadBalancingStrategy findByName(String name);

    @Modifying
    void deleteByName(String strategyName);

    @Modifying
    @Query("update LoadBalancingStrategy s set s.factoryBean = ?2 where s.name = ?1")
    void updateFactoryBean(String strategyName, String factoryBean);

    @Modifying
    @Query("update LoadBalancingStrategy s set s.label = ?2 where s.name = ?1")
    void updateLabel(String strategyName, String label);
}
