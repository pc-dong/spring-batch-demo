package cn.dpc.ecommerce.batch.config;

import cn.dpc.ecommerce.batch.campaign.Campaign;
import cn.dpc.ecommerce.batch.campaign.CampaignAssociation;
import cn.dpc.ecommerce.batch.campaign.CampaignAssociationItemReader;
import cn.dpc.ecommerce.batch.campaign.CampaignAssociationItemWriter;
import cn.dpc.ecommerce.batch.campaign.CampaignItemReader;
import cn.dpc.ecommerce.batch.campaign.CampaignItemWriter;
import cn.dpc.ecommerce.batch.campaign.CampaignOffer;
import cn.dpc.ecommerce.batch.campaign.CampaignOfferItemReader;
import cn.dpc.ecommerce.batch.campaign.CampaignOfferItemWriter;
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
public class CampaignJobConfiguration {
    @Bean
    @Qualifier("campaignJob")
    public Job campaignJob(JobRepository jobRepository,
                          Step campaignAssociationStep,
                          Step campaignAssociationUpdateTimeStep,
                          Step campaignStep,
                          Step campaignUpdateTimeStep,
                          Step campaignOfferStep,
                          Step campaignOfferUpdateTimeStep,
                          Step lastUpdateTimeSaveStep) {
        return new JobBuilder("campaignJob", jobRepository)
                .start(campaignAssociationUpdateTimeStep)
                .next(campaignAssociationStep)
                .next(lastUpdateTimeSaveStep)
                .next(campaignUpdateTimeStep)
                .next(campaignStep)
                .next(lastUpdateTimeSaveStep)
                .next(campaignOfferUpdateTimeStep)
                .next(campaignOfferStep)
                .next(lastUpdateTimeSaveStep)
                .listener(new MyJobExecutionListener())
                .build();
    }

    @Bean
    public Step campaignStep(JobRepository jobRepository,
                             DataSourceTransactionManager transactionManager,
                             ItemReader<Campaign> campaignItemReader,
                             ItemWriter<Campaign> campaignItemWriter) {
        return new StepBuilder("campaignStep", jobRepository)
                .<Campaign, Campaign>chunk(PAGE_SIZE, transactionManager)
                .reader(campaignItemReader)
                .writer(campaignItemWriter)
                .allowStartIfComplete(true)
                .build();
    }

    @Bean
    @Qualifier("campaignUpdateTimeStep")
    public Step campaignUpdateTimeStep(DataSource dataSource, OpenSearchProperties openSearchProperties) {
        return new LastUpdateTimeStep(dataSource, openSearchProperties.getAppName(), "campaigns");
    }

    @Bean
    public ItemReader<Campaign> campaignItemReader(DataSource marketingDataSource) {
        return new CampaignItemReader(marketingDataSource, PAGE_SIZE, FETCH_SIZE);
    }

    @Bean
    public ItemWriter<Campaign> campaignItemWriter(OpenSearchProperties openSearchProperties, DocumentClient documentClient) {
        return new CampaignItemWriter(openSearchProperties, documentClient);
    }

    @Bean
    public Step campaignOfferStep(JobRepository jobRepository,
                             DataSourceTransactionManager transactionManager,
                             ItemReader<CampaignOffer> campaignOfferItemReader,
                             ItemWriter<CampaignOffer> campaignOfferItemWriter) {
        return new StepBuilder("campaignOfferStep", jobRepository)
                .<CampaignOffer, CampaignOffer>chunk(PAGE_SIZE, transactionManager)
                .reader(campaignOfferItemReader)
                .writer(campaignOfferItemWriter)
                .allowStartIfComplete(true)
                .build();
    }

    @Bean
    @Qualifier("campaignOfferUpdateTimeStep")
    public Step campaignOfferUpdateTimeStep(DataSource dataSource, OpenSearchProperties openSearchProperties) {
        return new LastUpdateTimeStep(dataSource, openSearchProperties.getAppName(), "campaign_offers");
    }

    @Bean
    public ItemReader<CampaignOffer> campaignOfferItemReader(DataSource marketingDataSource) {
        return new CampaignOfferItemReader(marketingDataSource, PAGE_SIZE, FETCH_SIZE);
    }

    @Bean
    public ItemWriter<CampaignOffer> campaignOfferItemWriter(OpenSearchProperties openSearchProperties, DocumentClient documentClient) {
        return new CampaignOfferItemWriter(openSearchProperties, documentClient);
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
                .allowStartIfComplete(true)
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
