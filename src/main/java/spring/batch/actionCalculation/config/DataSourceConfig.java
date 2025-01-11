package spring.batch.actionCalculation.config;

import com.zaxxer.hikari.HikariConfig;
import com.zaxxer.hikari.HikariDataSource;
import lombok.extern.slf4j.Slf4j;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Primary;
import org.springframework.context.annotation.PropertySource;
import org.springframework.jdbc.support.JdbcTransactionManager;
import org.springframework.transaction.PlatformTransactionManager;

import javax.sql.DataSource;


@Configuration
@EnableConfigurationProperties
@PropertySource("classpath:application.properties")
public class DataSourceConfig {

    private static final Logger log =  LoggerFactory.getLogger(DataSourceConfig.class);

    @Bean(name="commonHikariConfig")
    @ConfigurationProperties(prefix="datasource.hikari.common")
    public HikariConfig commonHikariConfig(){
        return new HikariConfig();
    }

    @Bean("postgresDataSourceProperties")
    @ConfigurationProperties(prefix = "postgres.db")
    public CommonDataSourceProperties postgresDataSourceProperties(){
        return new CommonDataSourceProperties();
    }

    @Bean("mysqlDataSourceProperties")
    @ConfigurationProperties(prefix = "mysql.db")
    public CommonDataSourceProperties mysqlDataSourceProperties(){
        return new CommonDataSourceProperties();
    }

    @Bean("postgresDataSource")
    public DataSource postgresDataSource(
            @Qualifier("commonHikariConfig") HikariConfig hikariConfig,
            @Qualifier("postgresDataSourceProperties") CommonDataSourceProperties properties
    ){
        return buildDataSource(hikariConfig,properties);
    }

    @Bean("dataSource")
    public DataSource dataSource(
            @Qualifier("commonHikariConfig") HikariConfig hikariConfig,
            @Qualifier("mysqlDataSourceProperties") CommonDataSourceProperties properties
    ){
        return buildDataSource(hikariConfig,properties);
    }

    @Bean("postgresTransactionManager")
    public PlatformTransactionManager postgresTransactionManager(
            @Qualifier("postgresDataSource") DataSource dataSource
    ){
        JdbcTransactionManager jdbcTransactionManager = new JdbcTransactionManager();
        jdbcTransactionManager.setDataSource(dataSource);
        return jdbcTransactionManager;
    }

    @Primary
    @Bean(name = "transactionManager")
    public PlatformTransactionManager mysqlTransactionManager(
            @Qualifier("dataSource") DataSource dataSource
    ) {
        JdbcTransactionManager jdbcTransactionManager = new JdbcTransactionManager();
        jdbcTransactionManager.setDataSource(dataSource);
        return jdbcTransactionManager;
    }

    private static HikariDataSource buildDataSource(HikariConfig config, CommonDataSourceProperties properties){
        HikariConfig hikariConfig = new HikariConfig();
        config.copyStateTo(hikariConfig);
        hikariConfig.setJdbcUrl(properties.getUrl());
        hikariConfig.setUsername(properties.getUsername());
        hikariConfig.setPassword(properties.getPassword());
        hikariConfig.setPoolName(properties.getPoolName());
        hikariConfig.setDriverClassName(properties.getDriverClassName());
        return new HikariDataSource(hikariConfig);
    }


}
