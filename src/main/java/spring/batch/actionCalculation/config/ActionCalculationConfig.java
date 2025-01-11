package spring.batch.actionCalculation.config;

import org.apache.catalina.User;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.batch.core.Step;
import org.springframework.batch.core.StepExecution;
import org.springframework.batch.core.StepExecutionListener;
import org.springframework.batch.core.configuration.annotation.EnableBatchProcessing;
import org.springframework.batch.core.job.AbstractJob;
import org.springframework.batch.core.job.builder.JobBuilder;
import org.springframework.batch.core.launch.JobLauncher;
import org.springframework.batch.core.launch.support.TaskExecutorJobLauncher;
import org.springframework.batch.core.repository.JobRepository;
import org.springframework.batch.core.step.builder.StepBuilder;
import org.springframework.batch.item.ItemProcessor;
import org.springframework.batch.item.ItemReader;
import org.springframework.batch.item.ItemWriter;
import org.springframework.batch.item.database.JdbcPagingItemReader;
import org.springframework.batch.item.database.builder.JdbcBatchItemWriterBuilder;
import org.springframework.batch.item.database.builder.JdbcPagingItemReaderBuilder;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Import;
import org.springframework.core.task.SimpleAsyncTaskExecutor;
import org.springframework.core.task.TaskExecutor;
import org.springframework.transaction.PlatformTransactionManager;
import spring.batch.actionCalculation.constants.CommonConstants;
import spring.batch.actionCalculation.model.SessionAction;
import spring.batch.actionCalculation.model.UserScoreUpdate;

import javax.sql.DataSource;

@Configuration
@Import(DataSourceConfig.class)
public class ActionCalculationConfig {

    private static final Logger LOGGER = LoggerFactory.getLogger(ActionCalculationConfig.class);


    @Bean("simpleActionCalculationJob")
    public AbstractJob simpleActionCalculationJob(
            JobRepository jobRepository,
            @Qualifier("simpleActionCalculationStep") Step simpleActionCalculationStep
    ){
        return (AbstractJob) new JobBuilder("simpleActionCalculationJob", jobRepository)
                .start(simpleActionCalculationStep)
                .build();
    }



    @Bean("simpleActionCalculationStep")
    public Step simpleActionCalculationStep(
            JobRepository jobRepository,
            @Qualifier("postgresTransactionManager") PlatformTransactionManager transactionManager,
            @Qualifier("sessionActionReader") ItemReader<SessionAction> sessionActionReader,
            @Qualifier("userScoreUpdateWriter") ItemWriter<UserScoreUpdate> userScoreUpdateWriter
    ){
        return new StepBuilder("simpleActionCalculationStep", jobRepository)
                .<SessionAction, UserScoreUpdate>chunk(5, transactionManager)
                .reader(sessionActionReader)
                .processor(getSessionActionProcessor())
                .writer(userScoreUpdateWriter)
                .listener(beforeStepLoggerListener())
                .build();
    }


    @Bean("sessionActionReader")
    public ItemReader<SessionAction> sessionActionReader(
            @Qualifier("postgresDataSource")DataSource postgresDataSource
            ){
        return new JdbcPagingItemReaderBuilder<SessionAction>()
                .name("sessionActionReader")
                .dataSource(postgresDataSource)
                .queryProvider(SessionAction.selectAllSessionActionsProvider())
                .rowMapper(SessionAction.getSessionActionMapper())
                .pageSize(5)
                .build();

    }

    private static ItemProcessor<SessionAction, UserScoreUpdate> getSessionActionProcessor() {
        return sessionAction -> {
            if (CommonConstants.PLUS_TYPE.equals(sessionAction.actionType())) {
                return new UserScoreUpdate(sessionAction.userId(), sessionAction.amount(), 1d);
            } else if (CommonConstants.MULTI_TYPE.equals(sessionAction.actionType())) {
                return new UserScoreUpdate(sessionAction.userId(), 0d, sessionAction.amount());
            } else {
                throw new RuntimeException("Unknown session action record type: " + sessionAction.actionType());
            }
        };
    }

    @Bean
    public ItemWriter<UserScoreUpdate> userScoreUpdateWriter(
            @Qualifier("postgresDataSource") DataSource postgresDataSource
    ){
        return new JdbcBatchItemWriterBuilder<UserScoreUpdate>()
                .dataSource(postgresDataSource)
                .itemPreparedStatementSetter(UserScoreUpdate.UPDATE_USER_SCORE_PARAMETER_SETTER)
                .sql(UserScoreUpdate.UPDATE_USER_SCORE_QUERY)
                .build();
    }

    private static StepExecutionListener beforeStepLoggerListener(){
        return new StepExecutionListener(){
            @Override
            public void beforeStep(StepExecution stepExecution){
                LOGGER.info("Calculation step is about to start handling all session action records");
            }
        };
    }


    @Bean("asyncJobLauncher")
    public JobLauncher asyncJobLauncher(JobRepository jobRepository){
        TaskExecutorJobLauncher taskExecutorJobLauncher = new TaskExecutorJobLauncher();
        taskExecutorJobLauncher.setJobRepository(jobRepository);
        taskExecutorJobLauncher.setTaskExecutor(new SimpleAsyncTaskExecutor());
        return taskExecutorJobLauncher;
    }

}
