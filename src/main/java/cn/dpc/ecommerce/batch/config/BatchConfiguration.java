package cn.dpc.ecommerce.batch.config;

import cn.dpc.ecommerce.batch.time.LastUpdateTimeSaveStep;
import org.springframework.batch.core.Step;
import org.springframework.batch.core.configuration.annotation.EnableBatchProcessing;
import org.springframework.batch.core.configuration.support.DefaultBatchConfiguration;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.boot.jdbc.DataSourceBuilder;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.jdbc.datasource.DataSourceTransactionManager;

import javax.sql.DataSource;

@Configuration
@EnableBatchProcessing(databaseType = "MYSQL")
public class BatchConfiguration extends DefaultBatchConfiguration {

    public static final int PAGE_SIZE = 50;
    public static final int FETCH_SIZE = 1000;
    public static final int CHUNK_SIZE = 50;

    @Bean
    @Qualifier("lastUpdateTimeSaveStep")
    public Step lastUpdateTimeSaveStep(DataSource dataSource) {
        return new LastUpdateTimeSaveStep(dataSource);
    }

    @Bean(name = "dataSource")
    @ConfigurationProperties(prefix = "spring.datasource")
    public DataSource jobRepositoryDataSource() {
        return DataSourceBuilder.create().build();
    }

    @Bean
    public DataSourceTransactionManager transactionManager(DataSource masterDataSource) {
        return new DataSourceTransactionManager(masterDataSource);
    }

    @Bean
    @Qualifier("masterDataSource")
    @ConfigurationProperties(prefix = "spring.datasource.masterdata")
    public DataSource masterDataSource() {
        return DataSourceBuilder.create().build();
    }

    @Bean
    @Qualifier("marketingDataSource")
    @ConfigurationProperties(prefix = "spring.datasource.marketing")
    public DataSource marketingDataSource() {
        return DataSourceBuilder.create().build();
    }
}
