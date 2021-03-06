package spark.training.utils;

import org.apache.spark.ml.evaluation.RegressionEvaluator;
import org.apache.spark.ml.recommendation.ALS;
import org.apache.spark.ml.recommendation.ALSModel;
import org.apache.spark.sql.DataFrameNaFunctions;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import spark.training.model.ml.als.ALSRankedModel;

import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static spark.training.model.Rating.RATING_MOVIEID_ROW_INDEX;
import static spark.training.model.Rating.RATING_USERID_ROW_INDEX;

/**
 * @author <a href="mailto:francisco.j.gonzalez.barea@gmail.com">Francisco Gonzalez</a>
 */
public class ALSModelUtils {

    private static final Logger LOGGER = LoggerFactory.getLogger(ALSModelUtils.class);

    public static List<ALSModel> createALSModelsList(List<Integer> maxIterValues, List<Double> regParamValues, List<Integer> rankValues,
                                                     String userCol, String itemCol, String ratingCol, Dataset<Row> training) {
        return maxIterValues.stream()
                .flatMap(maxIter -> regParamValues.stream()
                        .flatMap(regParam -> rankValues.stream()
                                .map(rank -> createALSModel(maxIter, regParam, rank, userCol, itemCol, ratingCol, training))))
                .collect(Collectors.toList());
    }

    public static Double computeALSModelRMSE(ALSModel model, String metricName, String rating, String predictionCol, Dataset<Row> testData) {
        RegressionEvaluator evaluator = new RegressionEvaluator()
                .setMetricName(metricName)
                .setLabelCol(rating)
                .setPredictionCol(predictionCol);
        Dataset<Row> predictions = model.transform(testData);
        DataFrameNaFunctions nonNaNValuesDF = new DataFrameNaFunctions(predictions);
        Stream.of((Row[])predictions.take(50)).forEach(pred -> LOGGER.debug("Calculating RMSE for: {},{},{}", pred.getInt(RATING_MOVIEID_ROW_INDEX), pred.getInt(RATING_USERID_ROW_INDEX),
                pred.get(pred.fieldIndex("prediction"))));
        Double rmse = evaluator.evaluate(nonNaNValuesDF.drop());
        LOGGER.debug("Root-mean-square error = {}", rmse);
        return rmse;
    }

    public static ALSModel calculateBestALSModel(List<ALSModel> alsModelsList, String metricName, String rating, String predictionCol,
                                                 Dataset<Row> testData) {
        List<ALSRankedModel> rankedModels = alsModelsList.stream()
                .map(alsModel -> new ALSRankedModel(alsModel, computeALSModelRMSE(alsModel, metricName, rating, predictionCol,
                        testData)))
                .sorted((model1, model2) -> model1.getMetric().compareTo(model2.getMetric()))
                .collect(Collectors.toList());
        return rankedModels.get(rankedModels.size() - 1).getModel();
    }

    private static ALSModel createALSModel(Integer maxIter, Double regParam, Integer rank, String userCol, String itemCol,
                                           String ratingCol, Dataset<Row> training) {
        ALS als = new ALS()
                .setMaxIter(maxIter)
                .setRegParam(regParam)
                .setRank(rank)
                .setUserCol(userCol)
                .setItemCol(itemCol)
                .setRatingCol(ratingCol)
                .setPredictionCol("prediction");
        return als.fit(training);
    }
}
