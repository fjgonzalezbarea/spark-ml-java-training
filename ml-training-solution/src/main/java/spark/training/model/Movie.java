package spark.training.model;

import org.apache.spark.api.java.function.Function;
import org.apache.spark.sql.Row;

import java.io.Serializable;

/**
 * @author <a href="mailto:francisco.j.gonzalez.barea@gmail.com">Francisco Gonzalez</a>
 */
public class Movie implements Serializable {

    public static final Integer MOVIE_MOVIEID_ROW_INDEX = 0;
    public static final Integer MOVIE_MOVIENAME_ROW_INDEX = 1;

    private Integer movieId;

    private String movieName;

    public static final Function<String, Movie> fileLineToMovie = (Function<String, Movie>) line -> Movie.parseMovie(line);

    // Needed for Spark Encoders
    public Movie() {
    }

    public Movie(Integer movieId, String movieName) {
        this.movieId = movieId;
        this.movieName = movieName;
    }

    public Integer getMovieId() {
        return movieId;
    }

    public String getMovieName() {
        return movieName;
    }

    public void setMovieId(Integer movieId) {
        this.movieId = movieId;
    }

    public void setMovieName(String movieName) {
        this.movieName = movieName;
    }

    public static Movie parseMovie(String line) {
        String[] fields = line.split("::");
        if (fields.length != 3) {
            throw new IllegalArgumentException("Each line must contain 3 fields");
        }
        int movieId = Integer.parseInt(fields[0]);
        String movieName = fields[1];
        return new Movie(movieId, movieName);
    }

    public static Movie transform(Row row) {
        return new Movie(row.getInt(0), row.getString(1));
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        Movie movie = (Movie) o;

        if (movieId != null ? !movieId.equals(movie.movieId) : movie.movieId != null) return false;
        return movieName != null ? movieName.equals(movie.movieName) : movie.movieName == null;

    }

    @Override
    public int hashCode() {
        int result = movieId != null ? movieId.hashCode() : 0;
        result = 31 * result + (movieName != null ? movieName.hashCode() : 0);
        return result;
    }
}
