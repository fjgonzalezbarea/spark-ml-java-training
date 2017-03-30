# spark-ml-java-training
Spark ML training from Data Bricks, solved in Java language.

Taking as reference the Spark Summit 2014 : https://databricks-training.s3.amazonaws.com/index.html This project is focused on solving the Machine Learning training course but in Java language (the course was done for Python and Scala).

You can find a base code to start working with, and a final solution too (as reference).

#1 To follow the original course, try this link: https://databricks-training.s3.amazonaws.com/movie-recommendation-with-mllib.html.

#2 The rest of the stuff needed can be found here: https://github.com/databricks/spark-training

# Prerequisites

You will need the following tools/frameworks to get started with this course:

* Java 8
* Maven 3 (solution working with mvn v3.3.3)
* A java IDE (solution made in IntelliJ 2016.1.3)
* Spark v2 distribution (solution tested successfully on Spark v2.1.0): http://spark.apache.org/

# How to run the solution
* Build your own ratings using python based app included in the USB distribution for the course (from #2):

<code>python [spark-training-USB-root]/machine-learning/bin/rateMovies</code>

* Build the ml-training-solution module using the profile build-with-dependencies so that all dependencies are included in the "distribution":

<code>mvn clean install -Pbuild-with-dependencies</code>

* Output to this app is done using Slf4j Loggers. You can configure this as you prefer. E.g. you can redirect this module logs into an special file doing the following:
1. Go to [spark root directory]/conf directory
2. Edit log4j.properties.template
3. Add the following lines at the end of the file

<code>
# Log training app at DEBUG level and into file
</code>
<code>
log4j.logger.spark.training=DEBUG, training
</code>

<code>
# File configuration
</code>
<code>
log4j.appender.training=org.apache.log4j.RollingFileAppender
log4j.appender.training.File=logs/trainings.log
log4j.appender.training.layout=org.apache.log4j.PatternLayout
log4j.appender.training.layout.ConversionPattern=%d{yy/MM/dd HH:mm:ss} %p %c{1}: %m%n
</code>

4. Save it as log4j.properties

* Submit the application to Spark:

<code>
[spark root directory]/bin/spark-submit.cmd --driver-memory 2g --master local[4] --class spark.training.MovieLensALS target\ml-training-solution-1.0-SNAPSHOT-jar-with-dependencies.jar [movieLensFilesHomeDir] [personalRatingsFile]
</code>

* You will find you´re 50 recommendations in the logs/output.log file
