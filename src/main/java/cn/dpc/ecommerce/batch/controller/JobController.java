package cn.dpc.ecommerce.batch.controller;

import lombok.RequiredArgsConstructor;
import lombok.SneakyThrows;
import org.springframework.batch.core.Job;
import org.springframework.batch.core.JobExecution;
import org.springframework.batch.core.JobParametersBuilder;
import org.springframework.batch.core.explore.JobExplorer;
import org.springframework.batch.core.launch.JobExecutionNotRunningException;
import org.springframework.batch.core.launch.JobLauncher;
import org.springframework.batch.core.launch.JobOperator;
import org.springframework.batch.core.launch.NoSuchJobExecutionException;
import org.springframework.context.ApplicationContext;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import java.util.List;
import java.util.Set;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

@RestController
@RequiredArgsConstructor
@RequestMapping("/jobs")
public class JobController {

    private final JobLauncher jobLauncher;
    private final JobExplorer jobExplorer;
    private final JobOperator jobOperator;
    private final ApplicationContext applicationContext;
    private final ExecutorService executors = Executors.newCachedThreadPool();

    @GetMapping("/names")
    public List<String> getJobNames() {
        return jobExplorer.getJobNames();
    }

    @GetMapping("/{name}/instances")
    public Set<JobExecution> getJobNames(@PathVariable String name) {
        return jobExplorer.findRunningJobExecutions(name);
    }

    @PostMapping("/{jobName}/stop")
    @SneakyThrows
    public void run(@PathVariable String jobName){
        jobExplorer.findRunningJobExecutions(jobName)
                .forEach(jobExecution -> {
                    try {
                        jobOperator.stop(jobExecution.getId());
                    } catch (NoSuchJobExecutionException | JobExecutionNotRunningException e) {
                        throw new RuntimeException(e);
                    }
                });
    }

    @PostMapping("/{jobName}/run")
    public void launch(@PathVariable String jobName) {
       var job =  applicationContext.getBean(jobName, Job.class);

        executors.submit(() -> {
            try {
                jobLauncher.run(job, new JobParametersBuilder()
                        .addLong("run.id", System.currentTimeMillis())
                        .toJobParameters());
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
        });
    }
}
