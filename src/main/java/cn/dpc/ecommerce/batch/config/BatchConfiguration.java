package cn.dpc.ecommerce.batch.config;

import cn.dpc.ecommerce.batch.listener.MyJobExecutionListener;
import cn.dpc.ecommerce.batch.opensearch.OpenSearchProperties;
import cn.dpc.ecommerce.batch.product.Product;
import cn.dpc.ecommerce.batch.product.ProductItemReader;
import cn.dpc.ecommerce.batch.product.ProductItemWriter;
import cn.dpc.ecommerce.batch.time.LastUpdateTimeSaveStep;
import cn.dpc.ecommerce.batch.time.LastUpdateTimeStep;
import com.aliyun.opensearch.DocumentClient;
import org.springframework.batch.core.Job;
import org.springframework.batch.core.Step;
import org.springframework.batch.core.configuration.annotation.EnableBatchProcessing;
import org.springframework.batch.core.configuration.support.DefaultBatchConfiguration;
import org.springframework.batch.core.job.builder.JobBuilder;
import org.springframework.batch.core.repository.JobRepository;
import org.springframework.batch.core.step.builder.StepBuilder;
import org.springframework.batch.item.ItemReader;
import org.springframework.batch.item.ItemWriter;
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

    @Bean
    @Qualifier("masterJob")
    public Job masterJob(JobRepository jobRepository, Step masterStep, Step lastUpdateTimeStep, Step lastUpdateTimeSaveStep) {
        return new JobBuilder("masterJob", jobRepository)
                .start(lastUpdateTimeStep)
                .next(masterStep)
                .next(lastUpdateTimeSaveStep)
                .listener(new MyJobExecutionListener())
                .build();
    }

    @Bean
    public Step masterStep(JobRepository jobRepository, DataSourceTransactionManager transactionManager, ItemWriter<Product> productItemWriter) {
        return new StepBuilder("masterStep", jobRepository)
                .<Product, Product>chunk(10, transactionManager)
                .reader(masterItemReader())
                .writer(productItemWriter)
//                .allowStartIfComplete(true)
                .build();
    }

    @Bean
    public ItemReader<Product> masterItemReader() {
        return new ProductItemReader(masterDataSource(), 10, 10);
    }

    @Qualifier("productItemWriter")
    @Bean
    public ItemWriter<Product> productItemWriter(OpenSearchProperties openSearchProperties, DocumentClient documentClient) {
        return new ProductItemWriter(openSearchProperties, documentClient);
    }

    @Bean
    @Qualifier("lastUpdateTimeStep")
    public Step lastUpdateTimeStep(DataSource masterDataSource, OpenSearchProperties openSearchProperties) {
        return new LastUpdateTimeStep(masterDataSource, openSearchProperties.getAppName(), "product");
    }

    @Bean
    @Qualifier("lastUpdateTimeSaveStep")
    public Step lastUpdateTimeSaveStep(DataSource masterDataSource) {
        return new LastUpdateTimeSaveStep(masterDataSource);
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
