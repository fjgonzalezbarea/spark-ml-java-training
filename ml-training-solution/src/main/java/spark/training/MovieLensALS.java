package spark.training;

import com.google.common.collect.ImmutableMap;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.MapFunction;
import org.apache.spark.ml.recommendation.ALSModel;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoder;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.Row;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import spark.training.model.Movie;
import spark.training.model.Rating;
import spark.training.services.ApplicationService;

import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.stream.IntStream;

import static spark.training.model.Movie.fileLineToMovie;
import static spark.training.model.Rating.USERID_ROW_INDEX;
import static spark.training.model.Rating.fileLineToRating;
import static spark.training.utils.ALSModelUtils.chooseBestALSModel;
import static spark.training.utils.ALSModelUtils.createALSModelsList;
import static spark.training.utils.DataUtils.buildDataPartition;
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
        Map<String, String> configValues = ImmutableMap.<String, String>builder()
                .put("spark.executor.memory", "2g")
                .put("spark.network.timeout", "800")
                .build();
        ApplicationService service = new ApplicationService("MovieLensALS", args[0], configValues);

        // 3. Load Personal Ratings
        // 3.1 Create ML Service instance
        JavaRDD<Rating> myRatings = service.loadDatasetFromFile("personalRatings.txt", fileLineToRating);

        // 4. Load Ratings
        JavaRDD<Rating> ratings = service.loadDatasetFromFile("ratings.dat", fileLineToRating);

        // 5. Load movies
        JavaRDD<Movie> movies = service.loadDatasetFromFile("movies.dat", fileLineToMovie);

        // 6. TRAIN ML MODEL
        // 6.1 Splitting training data
        int numPartitions = 4;
        // IMPORTANT: Dataset<Row> are mapped based on class attrs, in alphabetical order
        Dataset<Row> ratingsDataset = service.transform(ratings, Rating.class);
        Dataset<Row> myRatingsDataset = service.transform(myRatings, Rating.class);
        Dataset<Row> training = buildDataPartition(ratingsDataset, myRatingsDataset, buildFilterFunctionLowerThan(USERID_ROW_INDEX, 6),
                numPartitions).cache();
        Dataset<Row> validation = buildDataPartition(ratingsDataset, buildFilterFunctionRange(USERID_ROW_INDEX, 6, 8),
                numPartitions).cache();
        Dataset<Row> test = buildDataPartition(ratingsDataset, buildFilterFunctionHigherOrEqualThan(USERID_ROW_INDEX, 8))
                .cache();

        // You should see 'Training: 602251, validation: 198919, test: 199049.'
        LOGGER.info("Training: {}, validation: {}, test: {}", training.count(), validation.count(), test.count());

        // 6.2 Create Models
        List<ALSModel> models = createALSModelsList(Arrays.asList(10, 20), Arrays.asList(1.0, 10.0), Arrays.asList(10, 20),
                "userId", "movieId", "rating", training);
        // 6.3 Evaluate model against test set of data
        ALSModel bestModel = chooseBestALSModel(models, "rmse", "rating", "prediction", test);

        // 7. CREATE RECOMMENDATIONS FOR YOU
        // 7.1 Build Candidates i.e. movies not ranked by you
        JavaRDD<Rating> candidates = ratings.subtract(myRatings);
        Dataset<Row> predictions = bestModel.transform(service.transform(candidates, Rating.class));

        System.out.println("Movies recommended for you:");
        Encoder<Movie> movieEncoder = Encoders.bean(Movie.class);
        Movie[] recommendations = (Movie[]) predictions
                .map(new MapFunction<Row, Movie>() {
                    @Override
                    public Movie call(Row row) throws Exception {
                        return Movie.transform(row);
                    }
                }, movieEncoder).take(50);

        IntStream.range(0, 49)
                .forEach(index -> System.out.println(index + ": " + recommendations[index].getMovieName()));

        // 7. Clean up and stop
        service.closeSession();
    }

    private static void validateAppArguments(String[] args) {
        if (args.length != 2) {
            LOGGER.error("Usage: [spark root directory]/bin/spark-submit --driver-memory 2g --class MovieLensALS " +
                    "target/scala-*/movielens-als-ssembly-*.jar movieLensHomeDir personalRatingsFile");
            System.exit(1);
        }
    }
}
