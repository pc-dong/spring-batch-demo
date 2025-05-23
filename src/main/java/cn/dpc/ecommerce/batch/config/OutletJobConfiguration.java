package cn.dpc.ecommerce.batch.config;

import cn.dpc.ecommerce.batch.listener.MyJobExecutionListener;
import cn.dpc.ecommerce.batch.location.Outlet;
import cn.dpc.ecommerce.batch.location.OutletAssociation;
import cn.dpc.ecommerce.batch.location.OutletAssociationItemReader;
import cn.dpc.ecommerce.batch.location.OutletAssociationItemWriter;
import cn.dpc.ecommerce.batch.location.OutletItemReader;
import cn.dpc.ecommerce.batch.location.OutletItemWriter;
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
public class OutletJobConfiguration {
    @Bean
    @Qualifier("outletJob")
    public Job outletJob(JobRepository jobRepository,
                          Step outletAssociationStep,
                          Step outletAssociationUpdateTimeStep,
                          Step lastUpdateTimeSaveStep,
                          Step outletStep,
                          Step outletUpdateTimeStep) {
        return new JobBuilder("outletAssociationJob", jobRepository)
                .start(outletAssociationUpdateTimeStep)
                .next(outletAssociationStep)
                .next(lastUpdateTimeSaveStep)
                .next(outletUpdateTimeStep)
                .next(outletStep)
                .next(lastUpdateTimeSaveStep)
                .listener(new MyJobExecutionListener())
                .build();
    }

    @Bean
    public Step outletAssociationStep(JobRepository jobRepository,
                            DataSourceTransactionManager transactionManager,
                            ItemReader<OutletAssociation> outletAssociationItemReader,
                            ItemWriter<OutletAssociation> outletAssociationItemWriter) {
        return new StepBuilder("outletAssociationStep", jobRepository)
                .<OutletAssociation, OutletAssociation>chunk(PAGE_SIZE, transactionManager)
                .reader(outletAssociationItemReader)
                .writer(outletAssociationItemWriter)
                .allowStartIfComplete(true)
                .build();
    }

    @Bean
    public ItemReader<OutletAssociation> outletAssociationItemReader(DataSource masterDataSource) {
        return new OutletAssociationItemReader(masterDataSource, PAGE_SIZE, FETCH_SIZE);
    }

    @Bean
    public ItemWriter<OutletAssociation> outletAssociationItemWriter(OpenSearchProperties openSearchProperties,
                                                DocumentClient documentClient) {
        return new OutletAssociationItemWriter(openSearchProperties, documentClient);
    }


    @Bean
    @Qualifier("outletAssociationUpdateTimeStep")
    public Step outletAssociationUpdateTimeStep(DataSource dataSource, OpenSearchProperties openSearchProperties) {
        return new LastUpdateTimeStep(dataSource, openSearchProperties.getAppName(), "outlet_association");
    }

    @Bean
    public Step outletStep(JobRepository jobRepository,
                            DataSourceTransactionManager transactionManager,
                            ItemReader<Outlet> outletItemReader,
                            ItemWriter<Outlet> outletItemWriter) {
        return new StepBuilder("outletStep", jobRepository)
                .<Outlet, Outlet>chunk(PAGE_SIZE, transactionManager)
                .reader(outletItemReader)
                .writer(outletItemWriter)
                .allowStartIfComplete(true)
                .build();
    }

    @Bean
    public ItemReader<Outlet> outletItemReader(DataSource masterDataSource) {
        return new OutletItemReader(masterDataSource, PAGE_SIZE, FETCH_SIZE);
    }

    @Bean
    public ItemWriter<Outlet> outletItemWriter(OpenSearchProperties openSearchProperties,
                                                                     DocumentClient documentClient) {
        return new OutletItemWriter(openSearchProperties, documentClient);
    }


    @Bean
    @Qualifier("outletUpdateTimeStep")
    public Step outletUpdateTimeStep(DataSource dataSource, OpenSearchProperties openSearchProperties) {
        return new LastUpdateTimeStep(dataSource, openSearchProperties.getAppName(), "outlets");
    }
}
