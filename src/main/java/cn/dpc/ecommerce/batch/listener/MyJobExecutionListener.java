package cn.dpc.ecommerce.batch.listener;

import org.springframework.batch.core.JobExecution;
import org.springframework.batch.core.JobExecutionListener;

public class MyJobExecutionListener implements JobExecutionListener {

    @Override
    public void afterJob(JobExecution jobExecution) {
        System.out.println("after Job:" + jobExecution.getJobInstance().getJobName());
    }
}
