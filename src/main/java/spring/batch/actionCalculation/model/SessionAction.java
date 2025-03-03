package spring.batch.actionCalculation.model;

import org.springframework.batch.item.database.Order;
import org.springframework.batch.item.database.support.PostgresPagingQueryProvider;
import org.springframework.jdbc.core.RowMapper;
import spring.batch.actionCalculation.constants.CommonConstants;


import java.util.Collections;

public record SessionAction(
        long id,
        long userId,
        String actionType,
        double amount
) {
    public static PostgresPagingQueryProvider selectAllSessionActionsProvider(){
        PostgresPagingQueryProvider postgresPagingQueryProvider = new PostgresPagingQueryProvider();
        postgresPagingQueryProvider.setSelectClause("id, user_id, action_type, amount");
        postgresPagingQueryProvider.setFromClause(CommonConstants.SESSION_ACTION_TABLE_NAME);
        postgresPagingQueryProvider.setSortKeys(Collections.singletonMap("id", Order.ASCENDING));
        return postgresPagingQueryProvider;
    }

    public static PostgresPagingQueryProvider selectPartitionOfSessionActionsProvider(
            int partitionCount, int partitionIndex
    ) {
        PostgresPagingQueryProvider postgresPagingQueryProvider = selectAllSessionActionsProvider();
        postgresPagingQueryProvider.setWhereClause("user_id % " + partitionCount + "=" + partitionIndex);
        return postgresPagingQueryProvider;
    }

    public static RowMapper<SessionAction> getSessionActionMapper(){
        return (rs, rowNum)-> new SessionAction(rs.getLong("id"), rs.getLong("user_id"),
                rs.getString("action_type"), rs.getDouble("amount"));
    }
}
