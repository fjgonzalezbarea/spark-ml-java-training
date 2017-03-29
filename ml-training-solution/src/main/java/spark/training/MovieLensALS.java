package spark.training;

import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.MapFunction;
import org.apache.spark.ml.recommendation.ALSModel;
import org.apache.spark.sql.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import spark.training.model.Movie;
import spark.training.model.Rating;
import spark.training.services.ApplicationService;

import java.util.Arrays;
import java.util.Calendar;
import java.util.List;
import java.util.stream.Collectors;

import static spark.training.model.Movie.MOVIE_MOVIEID_ROW_INDEX;
import static spark.training.model.Movie.fileLineToMovie;
import static spark.training.model.Rating.fileLineToRating;
import static spark.training.utils.ALSModelUtils.calculateBestALSModel;
import static spark.training.utils.ALSModelUtils.createALSModelsList;

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
        ApplicationService service = new ApplicationService("MovieLensALS", args[0]);

        // 3. Run App
        runRecommendationApp(service);


    }

    private static void validateAppArguments(String[] args) {
        if (args.length != 2) {
            LOGGER.error("Usage: [spark root directory]/bin/spark-submit --driver-memory 2g --class MovieLensALS " +
                    "target/scala-*/movielens-als-ssembly-*.jar movieLensHomeDir personalRatingsFile");
            System.exit(1);
        }
    }

    private static void runRecommendationApp(ApplicationService service) {
        // 3. LOAD DATA
        Dataset<Row> allRatings = loadAllRatedMovies(service);

        // 4. ML TRAINING
        ALSModel bestModel = chooseBestALSModel(allRatings);

        // 5. CREATE RECOMMENDATIONS FOR YOU
        JavaRDD<Movie> movies = service.loadDatasetFromFile("movies.dat", fileLineToMovie);
        List<Rating> bestRatings = createRecommendationsForYouList(service, bestModel, movies);

        // 6. PRINT RESULTS
        printResults(movies, bestRatings);

        LOGGER.debug("END!!");
    }

    private static Dataset<Row> loadAllRatedMovies(ApplicationService service) {
        // 3.1 JavaRDDs (my ratings and all rated movies)
        JavaRDD<Rating> myRatings = service.loadDatasetFromFile("personalRatings.txt", fileLineToRating);
        JavaRDD<Rating> ratings = service.loadDatasetFromFile("ratings.dat", fileLineToRating);

        // 3.2 Datasets
        // IMPORTANT: Dataset<Row> are mapped based on class attrs, in alphabetical order
        Dataset<Row> myRatingsDataset = service.transform(myRatings, Rating.class);
        return service.transform(ratings, Rating.class).union(myRatingsDataset);
    }

    private static ALSModel chooseBestALSModel(Dataset<Row> allRatings) {
        // 4.1 Create subsets for training and test of the model
        Dataset<Row>[] splits = allRatings.randomSplit(new double[]{0.8, 0.2});
        // 4.2 Create Models
        LOGGER.debug("Creating all models...");
        List<ALSModel> models = createALSModelsList(Arrays.asList(5, 10), Arrays.asList(1.0, 10.0), Arrays.asList(10, 20),
                "userId", "movieId", "rating", splits[0]);
        // 4.3 Evaluate model against test set of data
        LOGGER.debug("Evaluate and choose best model...");
        return calculateBestALSModel(models, "rmse", "rating", "prediction", splits[1]);
    }

    private static List<Rating> createRecommendationsForYouList(ApplicationService service, ALSModel bestModel, JavaRDD<Movie> movies) {
        // 5.1 Build Candidates i.e. movies which has not been ranked by you
        Dataset<Rating> candidates = buildCandidates(service, movies);
        // 7.2 Make the predictions
        Dataset<Row> predictions = bestModel.transform(candidates);
        // 7.3 Filter possible NaN values (take a look at: https://issues.apache.org/jira/browse/SPARK-14489)
        DataFrameNaFunctions filterNaNValuesFunction = new DataFrameNaFunctions(predictions);
        // 7.4 Build best rated movies list:
        return buildBestRatedMoviesList(filterNaNValuesFunction);
    }

    private static Dataset<Rating> buildCandidates(ApplicationService service, JavaRDD<Movie> movies) {
        Dataset<Row> moviesDataset = service.transform(movies, Movie.class);
        Encoder<Rating> ratingEncoder = Encoders.bean(Rating.class);
        return moviesDataset
                .map(new MapFunction<Row, Rating>() {
                    @Override
                    public Rating call(Row row) throws Exception {
                        return new Rating(0, row.getInt(MOVIE_MOVIEID_ROW_INDEX), 0.0, Calendar.getInstance().getTime().getTime());
                    }
                }, ratingEncoder);
    }

    private static List<Rating> buildBestRatedMoviesList(DataFrameNaFunctions filterNaNValuesFunction) {
        return Arrays.asList((Rating[]) filterNaNValuesFunction.drop().sort(new Column("prediction").desc())
                    .map(new MapFunction<Row, Rating>() {
                        @Override
                        public Rating call(Row row) throws Exception {
                            return Rating.transform(row);
                        }
                    }, Encoders.bean(Rating.class)).take(50));
    }

    private static void printResults(JavaRDD<Movie> movies, List<Rating> bestRatings) {
        List<Integer> recommendedMovieIds = bestRatings.stream()
                .map(Rating::getMovieId)
                .collect(Collectors.toList());

        LOGGER.debug("Movies recommended for you:");
        movies
                .filter((Function<Movie, Boolean>) movie -> recommendedMovieIds.contains(movie.getMovieId()))
                .sortBy((Function<Movie, Integer>) movie -> recommendedMovieIds.indexOf(movie.getMovieId()), true, 1)
                .foreach(movie ->
                        LOGGER.debug(recommendedMovieIds.indexOf(movie.getMovieId()) + ": " + movie.getMovieName()));
    }
}
