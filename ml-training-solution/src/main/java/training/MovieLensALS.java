package spark.training;

import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.mllib.recommendation.MatrixFactorizationModel;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import spark.training.model.Movie;
import spark.training.model.Rating;

import java.util.Collections;
import java.util.List;

import static spark.training.utils.TrainingUtils.*;

/**
 * @author <a href="mailto:francisco.j.gonzalez.barea@gmail.com">Francisco Gonzalez</a>
 *         <p>
 *         Main app class
 */
public class MovieLensALS {

    private static final Logger LOGGER = LoggerFactory.getLogger(MovieLensALS.class);

    public static void main(String[] args) {
        // 1. Validate Arguments
        validateAppArguments(args);

        // 2. Setup Spark environment
        JavaSparkContext sc = setUpEnvironment();
        SparkSession spark = buildSession(sc);

        // 3. Load Personal Ratings
        List<Rating> myRatings = loadMyRatings(args[1]);

        // 4. Load Ratings
        Dataset<Row> ratings = loadTimedRatingsFromFile(args[0], "ratings.dat", spark);

        // 5. Load movies
        Dataset<Row> movies = loadMoviesFromFile(args[0], "movies.dat", spark);

        /**
         *
         *  6. Training code
         *
         */

        // 7. Clean up and stop
        spark.close();
        spark.stop();
    }

    /** TODO: As part of the exercises */
    private static List<Rating> loadMyRatings(String path) {
        return Collections.EMPTY_LIST;
    }

    /** TODO: As part of the exercises */
    private static Double computeRmse(MatrixFactorizationModel model, JavaRDD<Rating> data, Long n) {
        return 0.0;
    }
}
