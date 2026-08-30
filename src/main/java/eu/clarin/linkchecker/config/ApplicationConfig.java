package eu.clarin.linkchecker.config;

import org.springframework.boot.autoconfigure.EnableAutoConfiguration;


import org.springframework.boot.persistence.autoconfigure.EntityScan;
import org.springframework.context.annotation.ComponentScan;
import org.springframework.context.annotation.Configuration;

import org.springframework.context.annotation.Import;
import org.springframework.data.jpa.repository.config.EnableJpaRepositories;
import org.springframework.transaction.annotation.EnableTransactionManagement;

@Configuration
@EnableAutoConfiguration
@Import({
        org.springframework.boot.jdbc.autoconfigure.DataSourceAutoConfiguration.class,
        org.springframework.boot.hibernate.autoconfigure.HibernateJpaAutoConfiguration.class
})
@EntityScan("eu.clarin.linkchecker.persistence.model")
@EnableJpaRepositories("eu.clarin.linkchecker.persistence.repository")
@ComponentScan({"eu.clarin.linkchecker.persistence.service", "eu.clarin.linkchecker.persistence.generic"})
@EnableTransactionManagement
public class ApplicationConfig {
}
