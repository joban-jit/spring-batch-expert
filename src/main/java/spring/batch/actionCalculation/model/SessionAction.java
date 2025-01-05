package spring.batch.actionCalculation.model;

public record SessionAction(
        long id,
        long userId,
        String actionType,
        double amount
) {
}
