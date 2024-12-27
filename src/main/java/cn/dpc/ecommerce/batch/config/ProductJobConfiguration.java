package cn.dpc.ecommerce.batch.config;

import cn.dpc.ecommerce.batch.listener.MyJobExecutionListener;
import cn.dpc.ecommerce.batch.opensearch.OpenSearchProperties;
import cn.dpc.ecommerce.batch.product.BundleProductAssociationItemReader;
import cn.dpc.ecommerce.batch.product.FBProductAssociationItemReader;
import cn.dpc.ecommerce.batch.product.Product;
import cn.dpc.ecommerce.batch.product.ProductAssociationItemWriter;
import cn.dpc.ecommerce.batch.product.ProductAssociations;
import cn.dpc.ecommerce.batch.product.ProductItemReader;
import cn.dpc.ecommerce.batch.product.ProductItemWriter;
import cn.dpc.ecommerce.batch.product.RoomProductAssociationItemReader;
import cn.dpc.ecommerce.batch.product.SubProduct;
import cn.dpc.ecommerce.batch.product.SubProductItemReader;
import cn.dpc.ecommerce.batch.product.SubProductItemWriter;
import cn.dpc.ecommerce.batch.time.LastUpdateTimeStep;
import com.aliyun.opensearch.DocumentClient;
import org.springframework.batch.core.Job;
import org.springframework.batch.core.Step;
import org.springframework.batch.core.job.builder.JobBuilder;
import org.springframework.batch.core.repository.JobRepository;
import org.springframework.batch.core.step.builder.StepBuilder;
import org.springframework.batch.item.ItemReader;
import org.springframework.batch.item.ItemWriter;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.jdbc.datasource.DataSourceTransactionManager;

import javax.sql.DataSource;

import static cn.dpc.ecommerce.batch.config.BatchConfiguration.FETCH_SIZE;
import static cn.dpc.ecommerce.batch.config.BatchConfiguration.PAGE_SIZE;

@Configuration
public class ProductJobConfiguration {
    @Bean
    @Qualifier("productJob")
    public Job productJob(JobRepository jobRepository,
                          Step roomProductAssociationStep,
                          Step fbProductAssociationStep,
                          Step bundleProductAssociationStep,
                          Step roomProductAssociationsUpdateTimeStep,
                          Step fbProductAssociationsUpdateTimeStep,
                          Step bundleProductAssociationsUpdateTimeStep,
                          Step productUpdateTimeStep,
                          Step productStep,
                          Step subProductStep,
                          Step subProductUpdateTimeStep,
                          Step lastUpdateTimeSaveStep) {
        return new JobBuilder("productJob", jobRepository)
                .start(productUpdateTimeStep)
                .next(productStep)
                .next(lastUpdateTimeSaveStep)
                .next(subProductUpdateTimeStep)
                .next(subProductStep)
                .next(lastUpdateTimeSaveStep)
                .next(roomProductAssociationsUpdateTimeStep)
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
    public Step productStep(JobRepository jobRepository,
                            DataSourceTransactionManager transactionManager,
                            ItemReader<Product> productItemReader,
                            ItemWriter<Product> productItemWriter) {
        return new StepBuilder("productStep", jobRepository)
                .<Product, Product>chunk(PAGE_SIZE, transactionManager)
                .reader(productItemReader)
                .writer(productItemWriter)
                .build();
    }

    @Bean
    public Step subProductStep(JobRepository jobRepository,
                               DataSourceTransactionManager transactionManager,
                               ItemReader<SubProduct> subProductItemReader,
                               ItemWriter<SubProduct> subProductItemWriter) {
        return new StepBuilder("subProductStep", jobRepository)
                .<SubProduct, SubProduct>chunk(PAGE_SIZE, transactionManager)
                .reader(subProductItemReader)
                .writer(subProductItemWriter)
                .build();
    }

    @Bean
    public Step roomProductAssociationStep(JobRepository jobRepository,
                                           DataSourceTransactionManager transactionManager,
                                           ItemReader<ProductAssociations> roomProductAssociationItemReader,
                                           ItemWriter<ProductAssociations> productAssociationItemWriter) {
        return new StepBuilder("roomProductAssociationStep", jobRepository)
                .<ProductAssociations, ProductAssociations>chunk(PAGE_SIZE, transactionManager)
                .reader(roomProductAssociationItemReader)
                .writer(productAssociationItemWriter)
                .build();
    }

    @Bean
    public Step fbProductAssociationStep(JobRepository jobRepository,
                                         DataSourceTransactionManager transactionManager,
                                         ItemReader<ProductAssociations> fbProductAssociationItemReader,
                                         ItemWriter<ProductAssociations> productAssociationItemWriter) {
        return new StepBuilder("fbProductAssociationStep", jobRepository)
                .<ProductAssociations, ProductAssociations>chunk(PAGE_SIZE, transactionManager)
                .reader(fbProductAssociationItemReader)
                .writer(productAssociationItemWriter)
                .build();
    }

    @Bean
    public Step bundleProductAssociationStep(JobRepository jobRepository,
                                             DataSourceTransactionManager transactionManager,
                                                ItemReader<ProductAssociations> bundleProductAssociationItemReader,
                                             ItemWriter<ProductAssociations> productAssociationItemWriter) {
        return new StepBuilder("bundleProductAssociationStep", jobRepository)
                .<ProductAssociations, ProductAssociations>chunk(PAGE_SIZE, transactionManager)
                .reader(bundleProductAssociationItemReader)
                .writer(productAssociationItemWriter)
                .build();
    }

    @Bean
    public ItemReader<Product> productItemReader(DataSource masterDataSource) {
        return new ProductItemReader(masterDataSource, PAGE_SIZE, FETCH_SIZE);
    }

    @Bean
    public ItemWriter<Product> productItemWriter(OpenSearchProperties openSearchProperties,
                                                DocumentClient documentClient) {
        return new ProductItemWriter(openSearchProperties, documentClient);
    }

    @Bean
    public ItemReader<SubProduct> subProductItemReader(DataSource masterDataSource) {
        return new SubProductItemReader(masterDataSource, PAGE_SIZE, FETCH_SIZE);
    }

    @Bean
    public ItemWriter<SubProduct> subProductItemWriter(OpenSearchProperties openSearchProperties,
                                                       DocumentClient documentClient) {
        return new SubProductItemWriter(openSearchProperties, documentClient);
    }

    @Bean
    public ItemReader<ProductAssociations> roomProductAssociationItemReader(DataSource masterDataSource) {
        return new RoomProductAssociationItemReader(masterDataSource, PAGE_SIZE, FETCH_SIZE);
    }

    @Bean
    public ItemReader<ProductAssociations> fbProductAssociationItemReader(DataSource masterDataSource) {
        return new FBProductAssociationItemReader(masterDataSource, PAGE_SIZE, FETCH_SIZE);
    }

    @Bean
    public ItemReader<ProductAssociations> bundleProductAssociationItemReader(DataSource masterDataSource) {
        return new BundleProductAssociationItemReader(masterDataSource, PAGE_SIZE, PAGE_SIZE);
    }

    @Bean
    public ItemWriter<ProductAssociations> productAssociationItemWriter(OpenSearchProperties openSearchProperties,
                                                                        DocumentClient documentClient) {
        return new ProductAssociationItemWriter(openSearchProperties, documentClient);
    }


    @Bean
    @Qualifier("productUpdateTimeStep")
    public Step productUpdateTimeStep(DataSource dataSource, OpenSearchProperties openSearchProperties) {
        return new LastUpdateTimeStep(dataSource, openSearchProperties.getAppName(), "products");
    }

    @Bean
    @Qualifier("subProductUpdateTimeStep")
    public Step subProductUpdateTimeStep(DataSource dataSource, OpenSearchProperties openSearchProperties) {
        return new LastUpdateTimeStep(dataSource, openSearchProperties.getAppName(), "sub_products");
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
}
