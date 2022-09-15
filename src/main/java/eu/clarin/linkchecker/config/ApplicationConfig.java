package eu.clarin.linkchecker.config;

import org.springframework.boot.autoconfigure.EnableAutoConfiguration;
import org.springframework.boot.autoconfigure.domain.EntityScan;
import org.springframework.context.annotation.ComponentScan;
import org.springframework.context.annotation.Configuration;
import org.springframework.data.jpa.repository.config.EnableJpaRepositories;
import org.springframework.transaction.annotation.EnableTransactionManagement;

@ComponentScan(basePackages = {"eu.clarin.cmdi.cpa"})
@EnableJpaRepositories("eu.clarin.cmdi.cpa.repository")
@EntityScan("eu.clarin.cmdi.cpa.model")
@EnableAutoConfiguration
@Configuration
public class ApplicationConfig {

}
