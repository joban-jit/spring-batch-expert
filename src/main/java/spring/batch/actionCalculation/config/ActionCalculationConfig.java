package spring.batch.actionCalculation.config;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.batch.core.Job;
import org.springframework.batch.core.Step;
import org.springframework.batch.core.StepExecution;
import org.springframework.batch.core.StepExecutionListener;
import org.springframework.batch.core.job.builder.JobBuilder;
import org.springframework.batch.core.launch.JobLauncher;
import org.springframework.batch.core.launch.support.TaskExecutorJobLauncher;
import org.springframework.batch.core.repository.JobRepository;
import org.springframework.batch.core.step.builder.StepBuilder;
import org.springframework.batch.item.*;
import org.springframework.batch.item.database.JdbcBatchItemWriter;
import org.springframework.batch.item.database.builder.JdbcBatchItemWriterBuilder;
import org.springframework.batch.item.database.builder.JdbcPagingItemReaderBuilder;
import org.springframework.batch.item.support.SynchronizedItemStreamWriter;
import org.springframework.batch.item.support.builder.SynchronizedItemStreamReaderBuilder;
import org.springframework.batch.item.support.builder.SynchronizedItemStreamWriterBuilder;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.boot.task.ThreadPoolTaskExecutorBuilder;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Import;
import org.springframework.core.task.SimpleAsyncTaskExecutor;
import org.springframework.core.task.TaskExecutor;
import org.springframework.transaction.PlatformTransactionManager;
import spring.batch.actionCalculation.adaptor.JdbcBatchItemStreamWriterAdapter;
import spring.batch.actionCalculation.constants.CommonConstants;
import spring.batch.actionCalculation.model.SessionAction;
import spring.batch.actionCalculation.model.UserScoreUpdate;

import javax.sql.DataSource;
import java.util.concurrent.ThreadPoolExecutor;

@Configuration
@Import(DataSourceConfig.class)
public class ActionCalculationConfig {

    private static final Logger LOGGER = LoggerFactory.getLogger(ActionCalculationConfig.class);


    /*
    * Multithreaded job
     */
    @Bean("multiThreadedActionCalculationJob")
    public Job multiThreadedActionCalculationJob(
            JobRepository jobRepository,
            @Qualifier("multiThreadedActionCalculationStep") Step multiThreadedActionCalculationStep
    ){
        return new JobBuilder("multiThreadedActionCalculationJob", jobRepository)
                .start(multiThreadedActionCalculationStep)
                .build();
    }




    /*
    * Single threaded job
     */
    @Bean("simpleActionCalculationJob")
    public Job simpleActionCalculationJob(
            JobRepository jobRepository,
            @Qualifier("simpleActionCalculationStep") Step simpleActionCalculationStep
    ){
        return  new JobBuilder("simpleActionCalculationJob", jobRepository)
                .start(simpleActionCalculationStep)
                .build();
    }


    @Bean("multiThreadedActionCalculationStep")
    public Step multiThreadedActionCalculationStep(
            JobRepository jobRepository,
            @Qualifier("postgresTransactionManager") PlatformTransactionManager transactionManager,
            @Qualifier("synchronizedSessionActionReader") ItemStreamReader<SessionAction> synchronizedSessionActionReader,
            @Qualifier("synchronizedUserScoreUpdateWriter") SynchronizedItemStreamWriter<UserScoreUpdate> synchronizedUserScoreUpdateWriter,
            @Qualifier("multiThreadStepExecutor") TaskExecutor multiThreadStepExecutor,
            @Qualifier("beforeStepLoggerListener") StepExecutionListener beforeStepLoggerListener

    ){
        return new StepBuilder("multiThreadedActionCalculationStep", jobRepository)
                .<SessionAction, UserScoreUpdate>chunk(5, transactionManager)
                // Read in pages of size 5 using synchronized reader
                // to mitigate thread safety problems while using shared reader between threads
                .reader(synchronizedSessionActionReader)
                // Convert items into user score update objects used to update with (score = score * a + b) idea
                .processor(getSessionActionProcessor())
                // Write into the database using the upsert capabilities;
                // Using synchronized writer to avoid deadlock issues with Postgres.
                .writer(synchronizedUserScoreUpdateWriter)
                .listener(beforeStepLoggerListener)
                .taskExecutor(multiThreadStepExecutor)
                .build();
    }


    @Bean("simpleActionCalculationStep")
    public Step simpleActionCalculationStep(
            JobRepository jobRepository,
            @Qualifier("postgresTransactionManager") PlatformTransactionManager transactionManager,
            @Qualifier("sessionActionReader") ItemStreamReader<SessionAction> sessionActionReader,
            @Qualifier("userScoreUpdateWriter") ItemWriter<UserScoreUpdate> userScoreUpdateWriter,
            @Qualifier("beforeStepLoggerListener") StepExecutionListener beforeStepLoggerListener
    ){
        return new StepBuilder("simpleActionCalculationStep", jobRepository)
                .<SessionAction, UserScoreUpdate>chunk(5, transactionManager)
                .reader(sessionActionReader)
                .processor(getSessionActionProcessor())
                .writer(userScoreUpdateWriter)
                .listener(beforeStepLoggerListener)
                .build();
    }

    @Bean("synchronizedSessionActionReader")
    public ItemStreamReader<SessionAction> synchronizedSessionActionReader(
        @Qualifier("sessionActionReader") ItemStreamReader<SessionAction> sessionActionReader
    ){
        return new SynchronizedItemStreamReaderBuilder<SessionAction>()
                .delegate(sessionActionReader)
                .build();
    }

    @Bean
    @Qualifier("synchronizedUserScoreUpdateWriter")
    public SynchronizedItemStreamWriter<UserScoreUpdate> synchronizedUserScoreUpdateWriter(
            @Qualifier("userScoreUpdateWriter") ItemWriter<UserScoreUpdate> userScoreUpdateWriter
    ){
        var jdbcBatchItemStreamWriterAdapter = new JdbcBatchItemStreamWriterAdapter<UserScoreUpdate>((JdbcBatchItemWriter<UserScoreUpdate>) userScoreUpdateWriter);
        return new SynchronizedItemStreamWriterBuilder<UserScoreUpdate>()
                .delegate(jdbcBatchItemStreamWriterAdapter)
                .build();
    }


    @Bean("sessionActionReader")
    public ItemStreamReader<SessionAction> sessionActionReader(
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


    @Bean("beforeStepLoggerListener")
    public StepExecutionListener beforeStepLoggerListener(){
        return new StepExecutionListener(){
            @Override
            public void beforeStep(StepExecution stepExecution){
                LOGGER.info("Calculation step is about to start handling all session action records");
            }
        };
    }

    @Bean("multiThreadStepExecutor")
    public TaskExecutor multiThreadStepExecutor(){
        return new ThreadPoolTaskExecutorBuilder()
                .corePoolSize(3)
                .build();
    }


    @Bean("asyncJobLauncher")
    public JobLauncher asyncJobLauncher(JobRepository jobRepository){
        TaskExecutorJobLauncher taskExecutorJobLauncher = new TaskExecutorJobLauncher();
        taskExecutorJobLauncher.setJobRepository(jobRepository);
        taskExecutorJobLauncher.setTaskExecutor(new SimpleAsyncTaskExecutor());
        return taskExecutorJobLauncher;
    }



}
