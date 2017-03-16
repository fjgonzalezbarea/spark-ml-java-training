package spark.training.model;

import java.io.Serializable;

/**
 * @author <a href="mailto:francisco.j.gonzalez.barea@gmail.com">Francisco Gonzalez</a>
 */
public class Rating implements Serializable {

    private final Integer userId;

    private final Integer movieId;

    private final Double rating;

    private final Long timeStamp;

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
}
