package spark.training.model;

/**
 * @author <a href="mailto:francisco.j.gonzalez.barea@gmail.com">Francisco Gonzalez</a>
 */
public class Movie {

    private final Integer movieId;

    private final String movieName;

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
}
