package sparkml;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.ml.feature.VectorAssembler;
import org.apache.spark.ml.regression.LinearRegression;
import org.apache.spark.ml.regression.LinearRegressionModel;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

public class SparkML1 {

	public static void main(String[] args) {
		// TODO Auto-generated method stub
		long start = System.currentTimeMillis();
		Logger.getLogger("org.apache").setLevel(Level.WARN);
		// TODO Auto-generated method stub
		String logFile = "src/main/resources/GymCompetition.csv";
		SparkSession spark = SparkSession.builder().appName("Spark ML").config("spark.master", "local[*]")
				.getOrCreate();
		Dataset<Row> dataset = spark.read()
				.option("header", true)
				.option("inferSchema",  true)
				.csv(logFile).cache();

		//dataset.printSchema();
		
		VectorAssembler va = new VectorAssembler()
				.setInputCols(new String[] {"Age", "Height", "Weight"})
				.setOutputCol("features");
		dataset = va.transform(dataset);

		//dataset.show();
		
		Dataset<Row> modelInputData = dataset.select("NoOfReps", "features").withColumnRenamed("NoOfReps", "label");
		
		//Label and Featurees
		modelInputData.show();
		
		//Split it into 80% for training and 20 for Test-Eval
		Dataset<Row>[] trnAndTst = modelInputData.randomSplit(new double[] {0.8,0.2});
		
		//MODEL Fitting using training
		LinearRegression lr = new LinearRegression();
		LinearRegressionModel lrm = lr.fit(trnAndTst[0]);
		
		//PREDICT using test.
		lrm.transform(trnAndTst[1]).show();
		/*
		 * +-----+-----------------+------------------+
|label|         features|        prediction|
+-----+-----------------+------------------+
|   45|[23.0,163.0,65.0]| 48.76675091010117|
|   49|[20.0,172.0,79.0]| 53.30229003930186|
|   52|[24.0,180.0,78.0]| 51.84150239645719|
|   53|[23.0,174.0,74.0]|50.994182120358246|
+-----+-----------------+------------------+
		 */
		
		
	}

}
