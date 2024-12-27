package cn.dpc.ecommerce.batch.config;

import cn.dpc.ecommerce.batch.listener.MyJobExecutionListener;
import cn.dpc.ecommerce.batch.location.Property;
import cn.dpc.ecommerce.batch.location.PropertyItemReader;
import cn.dpc.ecommerce.batch.location.PropertyItemWriter;
import cn.dpc.ecommerce.batch.opensearch.OpenSearchProperties;
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
public class PropertyJobConfiguration {
    @Bean
    @Qualifier("propertyJob")
    public Job propertyJob(JobRepository jobRepository,
                          Step propertyStep,
                          Step propertyUpdateTimeStep,
                          Step lastUpdateTimeSaveStep) {
        return new JobBuilder("propertyJob", jobRepository)
                .start(propertyUpdateTimeStep)
                .next(propertyStep)
                .next(lastUpdateTimeSaveStep)
                .listener(new MyJobExecutionListener())
                .build();
    }

    @Bean
    public Step propertyStep(JobRepository jobRepository,
                            DataSourceTransactionManager transactionManager,
                            ItemReader<Property> propertyItemReader,
                            ItemWriter<Property> propertyItemWriter) {
        return new StepBuilder("propertyStep", jobRepository)
                .<Property, Property>chunk(PAGE_SIZE, transactionManager)
                .reader(propertyItemReader)
                .writer(propertyItemWriter)
                .allowStartIfComplete(true)
                .build();
    }


    @Bean
    public ItemReader<Property> propertyItemReader(DataSource masterDataSource) {
        return new PropertyItemReader(masterDataSource, PAGE_SIZE, FETCH_SIZE);
    }

    @Bean
    public ItemWriter<Property> propertyItemWriter(OpenSearchProperties openSearchProperties,
                                                DocumentClient documentClient) {
        return new PropertyItemWriter(openSearchProperties, documentClient);
    }


    @Bean
    @Qualifier("propertyUpdateTimeStep")
    public Step propertyUpdateTimeStep(DataSource dataSource, OpenSearchProperties openSearchProperties) {
        return new LastUpdateTimeStep(dataSource, openSearchProperties.getAppName(), "properties");
    }
}
