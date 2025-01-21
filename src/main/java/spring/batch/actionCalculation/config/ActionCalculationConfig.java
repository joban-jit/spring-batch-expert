package spring.batch.actionCalculation.config;

import org.apache.catalina.User;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.batch.core.Job;
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
import org.springframework.batch.item.ItemStreamReader;
import org.springframework.batch.item.ItemWriter;
import org.springframework.batch.item.database.JdbcPagingItemReader;
import org.springframework.batch.item.database.builder.JdbcBatchItemWriterBuilder;
import org.springframework.batch.item.database.builder.JdbcPagingItemReaderBuilder;
import org.springframework.batch.item.support.builder.SynchronizedItemStreamReaderBuilder;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.boot.task.ThreadPoolTaskExecutorBuilder;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Import;
import org.springframework.core.task.SimpleAsyncTaskExecutor;
import org.springframework.core.task.TaskExecutor;
import org.springframework.scheduling.concurrent.ThreadPoolTaskExecutor;
import org.springframework.transaction.PlatformTransactionManager;
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
            @Qualifier("userScoreUpdateWriter") ItemWriter<UserScoreUpdate> userScoreUpdateWriter,
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
                // Write into the database using the upsert capabilities; ideally, we should also wrap the writer
                // into synchronized writer to avoid deadlock issues with Postgres. However, we are not doing it,
                // since this step is provided for demonstration purposes anyway and will produce wrong results
                // because of strict order guarantee requirements because of the problem definition
                .writer(userScoreUpdateWriter)
                .listener(beforeStepLoggerListener())
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
