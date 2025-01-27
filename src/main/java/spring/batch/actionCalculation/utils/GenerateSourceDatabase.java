package spring.batch.actionCalculation.utils;


import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.context.annotation.AnnotationConfigApplicationContext;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.transaction.PlatformTransactionManager;
import org.springframework.transaction.support.TransactionTemplate;
import spring.batch.actionCalculation.config.DataSourceConfig;
import spring.batch.actionCalculation.constants.CommonConstants;
import spring.batch.actionCalculation.model.SessionAction;

import javax.sql.DataSource;
import java.util.Random;
import java.util.stream.IntStream;


public class GenerateSourceDatabase {
    private static final Logger log = LoggerFactory.getLogger(GenerateSourceDatabase.class);

    private static final Random random = new Random();

    public static void main(String[] args) {
        try(var context = new AnnotationConfigApplicationContext(DataSourceConfig.class)){
//            AnnotationConfigApplicationContext context = new AnnotationConfigApplicationContext(DataSourceConfig.class);
            var jdbcTemplate = new JdbcTemplate(context.getBean("postgresDataSource", DataSource.class));
            var postgresTransactionManager = context.getBean("postgresTransactionManager", PlatformTransactionManager.class);
            SourceDatabaseUtils.createNewSessionActionTable(jdbcTemplate, postgresTransactionManager);
            new TransactionTemplate(postgresTransactionManager).execute(status -> {
                IntStream.range(0, CommonConstants.RECORD_COUNT).forEach(i -> {
                    SourceDatabaseUtils.insertSessionAction(jdbcTemplate, generateRecord(i + 1), CommonConstants.SESSION_ACTION_TABLE_NAME);
                });
                return null;
            });
            log.info("Input source table with {} records is successfully initialized", CommonConstants.RECORD_COUNT);
        }
    }

    private static SessionAction generateRecord(long id){
        long userId = 1+random.nextInt(CommonConstants.USER_COUNT);
        return random.nextBoolean()
                ? new SessionAction(id, userId, CommonConstants.PLUS_TYPE, 1 + random.nextInt(3))
                : new SessionAction(id, userId, CommonConstants.MULTI_TYPE, ((double) (1 + random.nextInt(5))) / 10 + 1d);
    }
}
