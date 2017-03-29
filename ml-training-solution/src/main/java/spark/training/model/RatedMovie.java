package spark.training.model;

import spark.training.model.Movie;

/**
 * @author <a href="mailto:francisco.j.gonzalez.barea@gmail.com">Francisco Gonzalez</a>
 */
public class RatedMovie extends Movie {

    private Double rating;

    // Needed for Spark Encoders
    public RatedMovie() {
        // Do nothing;
    }

    public RatedMovie(Movie movie, Double rating) {
        this.setMovieId(movie.getMovieId());
        this.setMovieName(movie.getMovieName());
        this.rating = rating;
    }

    public Double getRating() {
        return rating;
    }

    public void setRating(Double rating) {
        this.rating = rating;
    }
}
