package spring.batch.actionCalculation.partitioner;

import org.springframework.batch.core.partition.support.Partitioner;
import org.springframework.batch.item.ExecutionContext;

import java.util.Map;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import static spring.batch.actionCalculation.constants.CommonConstants.*;

public class SessionActionPartitioner implements Partitioner {

    @Override
    public Map<String, ExecutionContext> partition(int gridSize) {
        return IntStream.range(0, gridSize).mapToObj(i->{
            ExecutionContext executionContext = new ExecutionContext();
            executionContext.putInt(PARTITION_COUNT, gridSize);
            executionContext.putInt(PARTITION_INDEX, i);
            return Map.entry(PARTITION_NAME_PREFIX + i, executionContext);
        }).collect(Collectors.toMap(Map.Entry::getKey,Map.Entry::getValue ));
    }
}
