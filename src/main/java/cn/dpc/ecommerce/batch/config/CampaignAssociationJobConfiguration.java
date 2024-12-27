package cn.dpc.ecommerce.batch.config;

import cn.dpc.ecommerce.batch.campaign.CampaignAssociation;
import cn.dpc.ecommerce.batch.campaign.CampaignAssociationItemReader;
import cn.dpc.ecommerce.batch.campaign.CampaignAssociationItemWriter;
import cn.dpc.ecommerce.batch.listener.MyJobExecutionListener;
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
public class CampaignAssociationJobConfiguration {
    @Bean
    @Qualifier("campaignAssociationJob")
    public Job campaignAssociationJob(JobRepository jobRepository,
                          Step campaignAssociationStep,
                          Step campaignAssociationUpdateTimeStep,
                          Step lastUpdateTimeSaveStep) {
        return new JobBuilder("campaignAssociationJob", jobRepository)
                .start(campaignAssociationUpdateTimeStep)
                .next(campaignAssociationStep)
                .next(lastUpdateTimeSaveStep)
                .listener(new MyJobExecutionListener())
                .build();
    }

    @Bean
    public Step campaignAssociationStep(JobRepository jobRepository,
                            DataSourceTransactionManager transactionManager,
                            ItemReader<CampaignAssociation> campaignAssociationItemReader,
                            ItemWriter<CampaignAssociation> campaignAssociationItemWriter) {
        return new StepBuilder("campaignAssociationStep", jobRepository)
                .<CampaignAssociation, CampaignAssociation>chunk(PAGE_SIZE, transactionManager)
                .reader(campaignAssociationItemReader)
                .writer(campaignAssociationItemWriter)
                .build();
    }


    @Bean
    public ItemReader<CampaignAssociation> campaignAssociationItemReader(DataSource masterDataSource) {
        return new CampaignAssociationItemReader(masterDataSource, PAGE_SIZE, FETCH_SIZE);
    }

    @Bean
    public ItemWriter<CampaignAssociation> campaignAssociationItemWriter(OpenSearchProperties openSearchProperties,
                                                DocumentClient documentClient) {
        return new CampaignAssociationItemWriter(openSearchProperties, documentClient);
    }


    @Bean
    @Qualifier("campaignAssociationUpdateTimeStep")
    public Step campaignAssociationUpdateTimeStep(DataSource dataSource, OpenSearchProperties openSearchProperties) {
        return new LastUpdateTimeStep(dataSource, openSearchProperties.getAppName(), "campaign_association");
    }
}
