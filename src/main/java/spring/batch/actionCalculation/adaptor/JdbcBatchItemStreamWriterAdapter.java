package spring.batch.actionCalculation.adaptor;

import org.springframework.batch.item.Chunk;
import org.springframework.batch.item.ItemStreamWriter;
import org.springframework.batch.item.database.JdbcBatchItemWriter;

public class JdbcBatchItemStreamWriterAdapter<T> implements ItemStreamWriter<T> {

    private final JdbcBatchItemWriter<T> jdbcBatchItemWriter;

    public JdbcBatchItemStreamWriterAdapter(JdbcBatchItemWriter<T> jdbcBatchItemWriter) {
        this.jdbcBatchItemWriter = jdbcBatchItemWriter;
    }


    @Override
    public void write(Chunk<? extends T> chunk) throws Exception {
            jdbcBatchItemWriter.write(chunk);
    }
}
