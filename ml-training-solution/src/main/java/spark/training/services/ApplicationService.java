package spark.training.services;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

import java.io.File;
import java.util.Collections;
import java.util.Map;

/**
 * @author <a href="mailto:francisco.j.gonzalez.barea@gmail.com">Francisco Gonzalez</a>
 */
public class ApplicationService {

    private final SparkSession spark;
    private final JavaSparkContext sc;
    private final String homeDirectory;

    public ApplicationService(String appName, String homeDirectory, Map<String, String> configProperties) {
        this.sc = setUpEnvironment(appName, configProperties);
        this.spark = buildSession(sc);
        this.homeDirectory = homeDirectory;
    }

    public void closeSession() {
        spark.close();
        spark.stop();
        sc.stop();
    }

    public <T> JavaRDD<T> loadDatasetFromFile(String fileName, Function<String, T> parseFunction) {
        return spark.read().textFile(new File(homeDirectory, fileName).toString())
                .javaRDD()
                .map(parseFunction);
    }

    public <T> Dataset<Row> transform(JavaRDD<T> javaRDDData, Class<T> type) {
        return spark.createDataFrame(javaRDDData, type);
    }

    private JavaSparkContext setUpEnvironment(String appName, Map<String, String> configProperties) {
        SparkConf conf = new SparkConf()
                .setAppName(appName);
        configProperties.keySet().forEach(key -> conf.set(key, configProperties.get(key)));
        return new JavaSparkContext(conf);
    }

    private SparkSession buildSession(JavaSparkContext sc) {
        return SparkSession.builder()
                .config(sc.getConf())
                .getOrCreate();
    }
}
