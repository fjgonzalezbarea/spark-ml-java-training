package spark.training.utils;

import org.apache.spark.api.java.function.FilterFunction;
import org.apache.spark.sql.Row;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * @author <a href="mailto:francisco.j.gonzalez.barea@gmail.com">Francisco Gonzalez</a>
 */
public class TrainingUtils {

    private static final Logger LOGGER = LoggerFactory.getLogger(TrainingUtils.class);

    public static final FilterFunction<Row> buildFilterFunctionLowerThan(Integer column, Integer higherLimit) {
        return (FilterFunction<Row>) row -> row.getInt(column) < higherLimit;
    }

    public static final FilterFunction<Row> buildFilterFunctionHigherOrEqualThan(Integer column, Integer lowerLimit) {
        return (FilterFunction<Row>) row -> row.getInt(column) >= lowerLimit;
    }

    public static final FilterFunction<Row> buildFilterFunctionRange(Integer column, Integer lowerLimit, Integer higherLimit) {
        return (FilterFunction<Row>) row -> row.getInt(column) >= lowerLimit && row.getInt(column) < higherLimit;
    }
}
