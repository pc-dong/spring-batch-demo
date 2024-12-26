package cn.dpc.ecommerce.batch.config;

import cn.dpc.ecommerce.batch.listener.MyJobExecutionListener;
import cn.dpc.ecommerce.batch.opensearch.OpenSearchProperties;
import cn.dpc.ecommerce.batch.product.BundleProductAssociationItemReader;
import cn.dpc.ecommerce.batch.product.FBProductAssociationItemReader;
import cn.dpc.ecommerce.batch.product.ProductAssociationItemWriter;
import cn.dpc.ecommerce.batch.product.ProductAssociations;
import cn.dpc.ecommerce.batch.product.RoomProductAssociationItemReader;
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

    public static final int PAGE_SIZE = 50;
    public static final int FETCH_SIZE = 50;

    @Bean
    @Qualifier("productJob")
    public Job productJob(JobRepository jobRepository,
                          Step roomProductAssociationStep,
                          Step fbProductAssociationStep,
                          Step bundleProductAssociationStep,
                          Step roomProductAssociationsUpdateTimeStep,
                          Step fbProductAssociationsUpdateTimeStep,
                          Step bundleProductAssociationsUpdateTimeStep,
                          Step lastUpdateTimeSaveStep) {
        return new JobBuilder("productJob", jobRepository)
                .start(roomProductAssociationsUpdateTimeStep)
                .next(roomProductAssociationStep)
                .next(lastUpdateTimeSaveStep)
                .next(fbProductAssociationsUpdateTimeStep)
                .next(fbProductAssociationStep)
                .next(lastUpdateTimeSaveStep)
                .next(bundleProductAssociationsUpdateTimeStep)
                .next(bundleProductAssociationStep)
                .next(lastUpdateTimeSaveStep)
                .listener(new MyJobExecutionListener())
                .build();
    }


    @Bean
    public Step roomProductAssociationStep(JobRepository jobRepository,
                                           DataSourceTransactionManager transactionManager,
                                           ItemWriter<ProductAssociations> productAssociationItemWriter) {
        return new StepBuilder("roomProductAssociationStep", jobRepository)
                .<ProductAssociations, ProductAssociations>chunk(PAGE_SIZE, transactionManager)
                .reader(roomProductAssociationItemReader())
                .writer(productAssociationItemWriter)
                .build();
    }

    @Bean
    public Step fbProductAssociationStep(JobRepository jobRepository,
                                         DataSourceTransactionManager transactionManager,
                                         ItemWriter<ProductAssociations> productAssociationItemWriter) {
        return new StepBuilder("fbProductAssociationStep", jobRepository)
                .<ProductAssociations, ProductAssociations>chunk(PAGE_SIZE, transactionManager)
                .reader(fbProductAssociationItemReader())
                .writer(productAssociationItemWriter)
                .build();
    }

    @Bean
    public Step bundleProductAssociationStep(JobRepository jobRepository,
                                             DataSourceTransactionManager transactionManager,
                                             ItemWriter<ProductAssociations> productAssociationItemWriter) {
        return new StepBuilder("bundleProductAssociationStep", jobRepository)
                .<ProductAssociations, ProductAssociations>chunk(PAGE_SIZE, transactionManager)
                .reader(bundleProductAssociationItemReader())
                .writer(productAssociationItemWriter)
                .build();
    }

    @Bean
    public ItemReader<ProductAssociations> roomProductAssociationItemReader() {
        return new RoomProductAssociationItemReader(masterDataSource(), PAGE_SIZE, FETCH_SIZE);
    }

    @Bean
    public ItemReader<ProductAssociations> fbProductAssociationItemReader() {
        return new FBProductAssociationItemReader(masterDataSource(), PAGE_SIZE, FETCH_SIZE);
    }

    @Bean
    public ItemReader<ProductAssociations> bundleProductAssociationItemReader() {
        return new BundleProductAssociationItemReader(masterDataSource(), PAGE_SIZE, PAGE_SIZE);
    }

    @Bean
    public ItemWriter<ProductAssociations> productAssociationItemWriter(OpenSearchProperties openSearchProperties,
                                                                        DocumentClient documentClient) {
        return new ProductAssociationItemWriter(openSearchProperties, documentClient);
    }


    @Bean
    @Qualifier("roomProductAssociationsUpdateTimeStep")
    public Step roomProductAssociationsUpdateTimeStep(DataSource dataSource, OpenSearchProperties openSearchProperties) {
        return new LastUpdateTimeStep(dataSource, openSearchProperties.getAppName(), "room_product_associations");
    }

    @Bean
    @Qualifier("fbProductAssociationsUpdateTimeStep")
    public Step fbProductAssociationsUpdateTimeStep(DataSource dataSource, OpenSearchProperties openSearchProperties) {
        return new LastUpdateTimeStep(dataSource, openSearchProperties.getAppName(), "fb_product_associations");
    }

    @Bean
    @Qualifier("bundleProductAssociationsUpdateTimeStep")
    public Step bundleProductAssociationsUpdateTimeStep(DataSource dataSource, OpenSearchProperties openSearchProperties) {
        return new LastUpdateTimeStep(dataSource, openSearchProperties.getAppName(), "bundle_product_associations");
    }

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
