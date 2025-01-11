package spring.batch.actionCalculation.utils;

import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.transaction.PlatformTransactionManager;
import org.springframework.transaction.support.TransactionTemplate;
import spring.batch.actionCalculation.constants.CommonConstants;
import spring.batch.actionCalculation.model.SessionAction;

import static spring.batch.actionCalculation.constants.CommonConstants.SESSION_ACTION_TABLE_NAME;
import static spring.batch.actionCalculation.constants.CommonConstants.USER_SCORE_TABLE_NAME;

public class SourceDatabaseUtils {

    private SourceDatabaseUtils() {
        // Utility class should not be instantiated
    }

    public static void createNewSessionActionTable(JdbcTemplate jdbcTemplate, PlatformTransactionManager transactionManager){
        TransactionTemplate transactionTemplate = new TransactionTemplate(transactionManager);
        transactionTemplate.execute(status->{
            jdbcTemplate.update("create table if not exists " + SESSION_ACTION_TABLE_NAME + " (" +
                    "id serial primary key," +
                    "user_id int not null," +
                    // Either 'plus' or 'multi'
                    "action_type varchar(36) not null," +
                    "amount numeric(10,2) not null" +
                    ")");
            return null;
        });
        transactionTemplate.execute(status -> {
            jdbcTemplate.update("truncate table " + SESSION_ACTION_TABLE_NAME);
            return null; // Explicitly commit
        });
    }

    public static void createUserScoreTable(JdbcTemplate jdbcTemplate, PlatformTransactionManager transactionManager){
        TransactionTemplate transactionTemplate = new TransactionTemplate(transactionManager);
        transactionTemplate.execute(status->{
            jdbcTemplate.update("create table if not exists " + USER_SCORE_TABLE_NAME + " (" +
                    "user_id int not null unique," +
                    "score numeric(10,2) not null" +
                    ")");
            return null;
        });
        transactionTemplate.execute(status -> {
            jdbcTemplate.update("truncate table " + USER_SCORE_TABLE_NAME);
            return null; // Explicitly commit
        });

    }

    public static void insertSessionAction(JdbcTemplate jdbcTemplate, SessionAction sessionAction, String tableName){

        try {
            jdbcTemplate.update(
                    "insert into " + tableName + " (id, user_id, action_type, amount) values (?, ?, ?, ?)",
                    sessionAction.id(), sessionAction.userId(), sessionAction.actionType(), sessionAction.amount()
            );
        } catch (Exception e) {
            throw new RuntimeException("Failed to insert record: " + sessionAction, e);
        }
    }
}
