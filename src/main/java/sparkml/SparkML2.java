package sparkml;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.ml.feature.VectorAssembler;
import org.apache.spark.ml.regression.LinearRegression;
import org.apache.spark.ml.regression.LinearRegressionModel;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

public class SparkML2 {

	public static void main(String[] args) {
		// TODO Auto-generated method stub
		long start = System.currentTimeMillis();
		Logger.getLogger("org.apache").setLevel(Level.WARN);
		// TODO Auto-generated method stub
		String logFile = "src/main/resources/kc_house_data.csv";
		SparkSession spark = SparkSession.builder().appName("HousePriceAnalysis").config("spark.master", "local[*]")
				.getOrCreate();
		Dataset<Row> dataset = spark.read()
				.option("header", true)
				.option("inferSchema",  true)
				.csv(logFile).cache();

		dataset.printSchema();
		//dataset.show();
		
		
		VectorAssembler va = new VectorAssembler()
				//.setInputCols(new String[] {"bedrooms", "bathrooms", "sqft_living"})
				.setInputCols(new String[] {"bedrooms", "bathrooms", "sqft_living", "sqft_lot", "floors", "grade"})
				.setOutputCol("features");
		dataset = va.transform(dataset);

		//dataset.show();
		
		Dataset<Row> modelInputData = dataset.select("price", "features").withColumnRenamed("price", "label");
		
		//Label and Features
		modelInputData.show();
		
		//Split it into 80% for training and 20 for Test-Eval
		Dataset<Row>[] trnAndTst = modelInputData.randomSplit(new double[] {0.8,0.2});
		Dataset<Row> trainingData = trnAndTst[0];
		Dataset<Row> testData = trnAndTst[1];
		//MODEL Fitting using training
		LinearRegression lr = new LinearRegression();
		
		LinearRegressionModel lrm = lr.fit(trainingData);
		
		System.out.println("Model Training R2="+lrm.summary().r2() + " RMSE = " + lrm.summary().rootMeanSquaredError());
		//PREDICT using test....
		
		//lrm.transform(testData).show();
		
		//Dont need to run transform and predict to run evaluate R2 rmse on test data
		System.out.println("Model TEST R2="+lrm.evaluate(testData).r2() + " RMSE = " + lrm.evaluate(testData).rootMeanSquaredError());
		
		 //R-squared and RMSE
		
		
		
	}

}
