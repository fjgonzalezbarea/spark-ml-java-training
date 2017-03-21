package spark.training.utils;

import org.apache.spark.api.java.function.FilterFunction;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;

/**
 * @author <a href="mailto:francisco.j.gonzalez.barea@gmail.com">Francisco Gonzalez</a>
 */
public class DataUtils {

    public static Dataset<Row> buildDataPartition(Dataset<Row> originalDataSet, FilterFunction<Row> predicate) {
        return originalDataSet
                .filter(predicate);

    }

    public static Dataset<Row> buildDataPartition(Dataset<Row> originalDataSet, FilterFunction<Row> predicate, Integer numPartitions) {
        return originalDataSet
                .filter(predicate)
                .repartition(numPartitions);

    }

    public static Dataset<Row> buildDataPartition(Dataset<Row> originalDataSet, Dataset<Row> unionDataSet, FilterFunction<Row> predicate,
                                           Integer numPartitions) {
        return originalDataSet
                .filter(predicate)
                .union(unionDataSet)
                .repartition(numPartitions);

    }
}
