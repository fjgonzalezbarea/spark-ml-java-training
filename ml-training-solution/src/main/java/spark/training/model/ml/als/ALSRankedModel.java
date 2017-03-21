package spark.training.model.ml.als;

import org.apache.spark.ml.recommendation.ALSModel;

/**
 * @author <a href="mailto:francisco.j.gonzalez.barea@gmail.com">Francisco Gonzalez</a>
 */
public final class ALSRankedModel {
    private ALSModel model;
    private Double metric;


    public ALSRankedModel(ALSModel model, Double metric) {
        this.model = model;
        this.metric = metric;
    }

    public ALSModel getModel() {
        return model;
    }

    public Double getMetric() {
        return metric;
    }
}
