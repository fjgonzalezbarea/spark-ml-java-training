package spark.training.model;

import org.apache.spark.api.java.function.Function;
import org.apache.spark.sql.Encoder;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.Row;

/**
 * @author <a href="mailto:francisco.j.gonzalez.barea@gmail.com">Francisco Gonzalez</a>
 */
public class Movie {

    private final Integer movieId;

    private final String movieName;

    public static final Encoder<Movie> movieEncoder = Encoders.bean(Movie.class);

    public static final Function<String, Movie> fileLineToMovie = (Function<String, Movie>) line -> Movie.parseMovie(line);

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

    public static Movie parseMovie(String line) {
        String[] fields = line.split("::");
        if (fields.length != 2) {
            throw new IllegalArgumentException("Each line must contain 4 fields");
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
