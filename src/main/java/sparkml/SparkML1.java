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
		
		//MODEL Fitting
		LinearRegression lr = new LinearRegression();
		LinearRegressionModel lrm = lr.fit(modelInputData);
		
		//PREDICT ( but we use same data to fit and evaluate...not really fair
		lrm.transform(modelInputData).show();
		
		//split data into training and test data ...NEXT
	}

}
