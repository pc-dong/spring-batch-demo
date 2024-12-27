package cn.dpc.ecommerce.batch.listener;

import lombok.extern.slf4j.Slf4j;
import org.springframework.batch.core.JobExecution;
import org.springframework.batch.core.JobExecutionListener;

import java.util.Optional;

@Slf4j
public class MyJobExecutionListener implements JobExecutionListener {

    public static final String EXECUTION_START_TIME = "executionStartTime";

    @Override
    public void beforeJob(JobExecution jobExecution) {
        jobExecution.getExecutionContext().put(EXECUTION_START_TIME, System.currentTimeMillis());
        log.info("before Job: {}", jobExecution.getJobInstance().getJobName());
    }

    @Override
    public void afterJob(JobExecution jobExecution) {
        long startTime = Optional.ofNullable(jobExecution.getExecutionContext().get(EXECUTION_START_TIME, Long.class)).orElse(0L);
        if(startTime != 0) {
            long endTime = System.currentTimeMillis();
            log.info("Job: {} took {} ms", jobExecution.getJobInstance().getJobName(), endTime - startTime);
        }
        log.info("after Job: {}", jobExecution.getJobInstance().getJobName());
    }
}
