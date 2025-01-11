package spring.batch.actionCalculation.model;


import org.springframework.batch.item.database.ItemPreparedStatementSetter;

import java.sql.Types;

import static spring.batch.actionCalculation.constants.CommonConstants.USER_SCORE_TABLE_NAME;

public record UserScoreUpdate(
        long userId,
        double add,
        double multiply
) {

    public static ItemPreparedStatementSetter<UserScoreUpdate> UPDATE_USER_SCORE_PARAMETER_SETTER = (item, ps) -> {
        ps.setObject(1, item.userId(), Types.BIGINT);
        ps.setObject(2, item.add(), Types.DOUBLE);
        ps.setObject(3, item.multiply(), Types.DOUBLE);
        ps.setObject(4, item.add(), Types.DOUBLE);
    };
    public static String UPDATE_USER_SCORE_QUERY = "insert into "+USER_SCORE_TABLE_NAME+" (user_id, score) values (?, ?) " +
            "on conflict (user_id) do " +
            "update set score = "+USER_SCORE_TABLE_NAME+".score * ? + ?";
}
