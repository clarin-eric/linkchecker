package eu.clarin.linkchecker.config;

import org.springframework.boot.autoconfigure.EnableAutoConfiguration;


import org.springframework.boot.persistence.autoconfigure.EntityScan;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.ComponentScan;
import org.springframework.context.annotation.Configuration;
import org.springframework.data.jpa.repository.config.EnableJpaRepositories;
import org.springframework.orm.jpa.hibernate.LocalSessionFactoryBean;

@ComponentScan(basePackages = {"eu.clarin.linkchecker.persistence"})
@EnableJpaRepositories("eu.clarin.linkchecker.persistence.repository")
@EntityScan("eu.clarin.linkchecker.persistence.model")
@EnableAutoConfiguration
@Configuration
public class ApplicationConfig {
}
