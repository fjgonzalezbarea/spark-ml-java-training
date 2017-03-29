package spark.training.model;

import org.apache.spark.api.java.function.Function;
import org.apache.spark.sql.Row;

import java.io.Serializable;

/**
 * @author <a href="mailto:francisco.j.gonzalez.barea@gmail.com">Francisco Gonzalez</a>
 */
public class Rating implements Serializable {

    public static final Integer RATING_USERID_ROW_INDEX = 3;
    public static final Integer RATING_MOVIEID_ROW_INDEX = 0;
    public static final Integer RATING_RATING_ROW_INDEX = 1;
    public static final Integer RATING_TIMESTAMP_ROW_INDEX = 2;

    private Integer userId;

    private Integer movieId;

    private Double rating;

    private Long timeStamp;

    public static final Function<String, Rating> fileLineToRating = (Function<String, Rating>) line -> Rating.parseRating(line);

    // Needed for Spark Encoders
    public Rating(){
        // Do nothing
    }

    public Rating(Integer userId, Integer movieId, Double rating, Long timeStamp) {
        this.userId = userId;
        this.movieId = movieId;
        this.rating = rating;
        this.timeStamp = timeStamp;
    }

    public Integer getUserId() {
        return userId;
    }

    public Integer getMovieId() {
        return movieId;
    }

    public Double getRating() {
        return rating;
    }

    public Long getTimeStamp() {
        return timeStamp;
    }

    public void setUserId(Integer userId) {
        this.userId = userId;
    }

    public void setMovieId(Integer movieId) {
        this.movieId = movieId;
    }

    public void setRating(Double rating) {
        this.rating = rating;
    }

    public void setTimeStamp(Long timeStamp) {
        this.timeStamp = timeStamp;
    }

    public static Rating parseRating(String str) {
        String[] fields = str.split("::");
        if (fields.length != 4) {
            throw new IllegalArgumentException("Each line must contain 4 fields");
        }
        int userId = Integer.parseInt(fields[0]);
        int movieId = Integer.parseInt(fields[1]);
        double rating = Double.parseDouble(fields[2]);
        long timestamp = Long.parseLong(fields[3]);
        return new Rating(userId, movieId, rating, timestamp);
    }

    public static Rating transform(Row row) {
        return new Rating(row.getInt(RATING_USERID_ROW_INDEX), row.getInt(RATING_MOVIEID_ROW_INDEX), row.getDouble(RATING_RATING_ROW_INDEX),
                row.getLong(RATING_TIMESTAMP_ROW_INDEX));
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        Rating rating1 = (Rating) o;

        if (userId != null ? !userId.equals(rating1.userId) : rating1.userId != null) return false;
        if (movieId != null ? !movieId.equals(rating1.movieId) : rating1.movieId != null) return false;
        if (rating != null ? !rating.equals(rating1.rating) : rating1.rating != null) return false;
        return timeStamp != null ? timeStamp.equals(rating1.timeStamp) : rating1.timeStamp == null;

    }

    @Override
    public int hashCode() {
        int result = userId != null ? userId.hashCode() : 0;
        result = 31 * result + (movieId != null ? movieId.hashCode() : 0);
        result = 31 * result + (rating != null ? rating.hashCode() : 0);
        result = 31 * result + (timeStamp != null ? timeStamp.hashCode() : 0);
        return result;
    }

    @Override
    public String toString() {
        return "Rating{" +
                "userId=" + userId +
                ", movieId=" + movieId +
                ", rating=" + rating +
                ", timeStamp=" + timeStamp +
                '}';
    }
}
