package io.axoniq.platform.application;

import io.axoniq.platform.application.jpa.Application;
import org.springframework.data.jpa.repository.JpaRepository;

import java.util.List;

/**
 * Created by marc on 7/13/2017.
 */
public interface ApplicationRepository extends JpaRepository<Application, Long> {

    Application findFirstByName(String name);

    List<Application> findAllByTokenPrefix(String prefix);
}
