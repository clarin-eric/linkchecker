package eu.clarin.linkchecker.config;

import org.springframework.boot.autoconfigure.EnableAutoConfiguration;

import org.springframework.boot.persistence.autoconfigure.EntityScan;
import org.springframework.context.annotation.ComponentScan;
import org.springframework.context.annotation.Configuration;

import org.springframework.data.jpa.repository.config.EnableJpaRepositories;
import org.springframework.data.repository.config.BootstrapMode;
import org.springframework.transaction.annotation.EnableTransactionManagement;

@EntityScan("eu.clarin.linkchecker.persistence.model")
@EnableJpaRepositories("eu.clarin.linkchecker.persistence.repository")
@ComponentScan({"eu.clarin.linkchecker.persistence.service", "eu.clarin.linkchecker.persistence.generic"})
@EnableTransactionManagement
@EnableAutoConfiguration
@Configuration
public class ApplicationConfig {
}
