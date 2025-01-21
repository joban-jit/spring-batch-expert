package spring.batch.actionCalculation.controller;

import org.springframework.batch.core.Job;
import org.springframework.batch.core.JobParameters;
import org.springframework.batch.core.JobParametersBuilder;
import org.springframework.batch.core.JobParametersInvalidException;
import org.springframework.batch.core.job.AbstractJob;
import org.springframework.batch.core.launch.JobLauncher;
import org.springframework.batch.core.repository.JobExecutionAlreadyRunningException;
import org.springframework.batch.core.repository.JobInstanceAlreadyCompleteException;
import org.springframework.batch.core.repository.JobRestartException;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.transaction.PlatformTransactionManager;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RestController;
import spring.batch.actionCalculation.utils.SourceDatabaseUtils;

import javax.sql.DataSource;
import java.util.UUID;


@RestController
public class ApplicationController {

    @Autowired
    @Qualifier("postgresDataSource")
    private DataSource dataSource;

    @Autowired
    @Qualifier("postgresTransactionManager")
    private PlatformTransactionManager transactionManager;

    @Autowired
    @Qualifier("asyncJobLauncher")
    private JobLauncher jobLauncher;

    @Autowired
    private Job simpleActionCalculationJob;

    @Autowired
    private Job multiThreadedActionCalculationJob;

    @PostMapping("/start-simple-local")
    public String startSimpleLocal() throws JobInstanceAlreadyCompleteException, JobExecutionAlreadyRunningException, JobParametersInvalidException, JobRestartException {
        prepareEmptyResultTable();
        jobLauncher.run(simpleActionCalculationJob,buildUniqueJobParameters());
        return "Successfully started!\n";

    }

    @PostMapping("/start-multi-threaded")
    public String startMultiThreaded() throws Exception{
        prepareEmptyResultTable();
        jobLauncher.run(multiThreadedActionCalculationJob, buildUniqueJobParameters());
        return "Successfully started!\n";
    }

    private void prepareEmptyResultTable(){
        JdbcTemplate jdbcTemplate = new JdbcTemplate(dataSource);
        SourceDatabaseUtils.createUserScoreTable(jdbcTemplate, transactionManager);
    }

    private static JobParameters buildUniqueJobParameters(){
        return new JobParametersBuilder()
                .addString(UUID.randomUUID().toString(), UUID.randomUUID().toString())
                .toJobParameters();
    }
}
