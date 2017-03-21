package spark.training.model;

import org.apache.spark.api.java.function.Function;
import org.apache.spark.sql.Encoder;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.Row;

import java.io.Serializable;

/**
 * @author <a href="mailto:francisco.j.gonzalez.barea@gmail.com">Francisco Gonzalez</a>
 */
public class Rating implements Serializable {

    private Integer userId;

    private Integer movieId;

    private Double rating;

    private Long timeStamp;

    // Encoders are created for Java beans
    public final Encoder<Rating> ratingEncoder = Encoders.bean(Rating.class);

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
        return new Rating(row.getInt(0), row.getInt(1), row.getDouble(2), row.getLong(3));
    }

    public static final Function<String, Rating> fileLineToRating = new Function<String, Rating>() {
        @Override
        public Rating call(String line) throws Exception {
            return Rating.parseRating(line);
        }
    };
}
