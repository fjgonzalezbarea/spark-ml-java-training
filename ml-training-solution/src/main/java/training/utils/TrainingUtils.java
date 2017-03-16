package spark.training.utils;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import spark.training.model.Movie;
import spark.training.model.Rating;

import java.io.File;

/**
 * @author <a href="mailto:francisco.j.gonzalez.barea@gmail.com">Francisco Gonzalez</a>
 */
public class TrainingUtils {

    private static final Logger LOGGER = LoggerFactory.getLogger(TrainingUtils.class);

    public static void validateAppArguments(String[] args) {
        if (args.length != 2) {
            LOGGER.error("Usage: [spark root directory]/bin/spark-submit --driver-memory 2g --class MovieLensALS " +
                    "target/scala-*/movielens-als-ssembly-*.jar movieLensHomeDir personalRatingsFile");
            System.exit(1);
        }
    }

    public static JavaSparkContext setUpEnvironment() {
        SparkConf conf = new SparkConf()
                .setAppName("MovieLensALS")
                .set("spark.executor.memory", "2g");
        return new JavaSparkContext(conf);
    }

    public static SparkSession buildSession(JavaSparkContext sc) {
        return SparkSession.builder()
                .config(sc.getConf())
                .getOrCreate();
    }

    public static Dataset<Row> loadTimedRatingsFromFile(String homeDirectory, String fileName, SparkSession spark) {
        JavaRDD<Rating> ratings = spark.read().textFile(new File(homeDirectory, fileName).toString())
                .javaRDD()
                // Needs Spark Function implementation as it has to be Serializable
                .map(new Function<String, Rating>() {
                    public Rating call(String line) {
                        return Rating.parseRating(line);
                    }
                });
        return spark.createDataFrame(ratings, Rating.class);

    }

    public static Dataset<Row> loadMoviesFromFile(String homeDirectory, String fileName, SparkSession spark) {
        JavaRDD<Movie> movies = spark.read().textFile(new File(homeDirectory, fileName).toString())
                .javaRDD()
                // Needs Spark Function implementation as it has to be Serializable
                .map(new Function<String, Movie>() {
                    @Override
                    public Movie call(String line) throws Exception {
                        return Movie.parseMovie(line);
                    }
                });
        return spark.createDataFrame(movies, Movie.class);
    }
}
