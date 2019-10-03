package sparkml;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.ml.evaluation.RegressionEvaluator;
import org.apache.spark.ml.feature.OneHotEncoderEstimator;
import org.apache.spark.ml.feature.StringIndexer;
import org.apache.spark.ml.feature.VectorAssembler;
import org.apache.spark.ml.param.ParamMap;
import org.apache.spark.ml.regression.LinearRegression;
import org.apache.spark.ml.regression.LinearRegressionModel;
import org.apache.spark.ml.tuning.ParamGridBuilder;
import org.apache.spark.ml.tuning.TrainValidationSplit;
import org.apache.spark.ml.tuning.TrainValidationSplitModel;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

public class SparkML4 {

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
		//describe the dataset num rows, mean, stddev etc
		dataset.describe().show();
		
		StringIndexer gIdxr = null;
		gIdxr = new StringIndexer().setInputCol("condition").setOutputCol("conditionIndex");
		dataset = gIdxr.fit(dataset).transform(dataset);
		gIdxr = new StringIndexer().setInputCol("grade").setOutputCol("gradeIndex");
		dataset = gIdxr.fit(dataset).transform(dataset);
		gIdxr = new StringIndexer().setInputCol("zipcode").setOutputCol("zipcodeIndex");
		dataset = gIdxr.fit(dataset).transform(dataset);
		
		OneHotEncoderEstimator ge = new OneHotEncoderEstimator().setInputCols(new String[] {"conditionIndex", "gradeIndex", "zipcodeIndex"})
				.setOutputCols(new String[] {"conditionVector", "gradeVector", "zipcodeVector"});
		dataset = ge.fit(dataset).transform(dataset);
		
		//dataset.show();
		
		
		VectorAssembler va = new VectorAssembler()
				//.setInputCols(new String[] {"bedrooms", "bathrooms", "sqft_living"})
				.setInputCols(new String[] {"bedrooms", "bathrooms", "sqft_living", "sqft_lot", "floors", "gradeVector", "conditionVector", "zipcodeVector"})
				.setOutputCol("features");
		dataset = va.transform(dataset);

		//dataset.show();
		
		Dataset<Row> modelInputData = dataset.select("price", "features").withColumnRenamed("price", "label");
		
		//Label and Features
		modelInputData.show();
		
		//Split it into 80% for training and 20 for Test-Eval
		Dataset<Row>[] datasplits = modelInputData.randomSplit(new double[] {0.8,0.2});
		Dataset<Row> trnAndTst = datasplits[0];
		Dataset<Row> holdoutData = datasplits[1];
		//MODEL Fitting using training
		LinearRegression lr = new LinearRegression();
		
		ParamGridBuilder pgb = new ParamGridBuilder();
		ParamMap[] pm = pgb.addGrid(lr.regParam(), new double[] {0.005, 0.01, 0.1, 0.5})
				.addGrid(lr.elasticNetParam(), new double[] {0,0.25,0.5,1})
				.build();
		
		TrainValidationSplit tvs = new TrainValidationSplit()
				.setEstimator(lr)
				.setEvaluator(new RegressionEvaluator().setMetricName("r2"))
				.setEstimatorParamMaps(pm)
				.setTrainRatio(0.8);
		TrainValidationSplitModel tvsm = tvs.fit(trnAndTst);
		
		LinearRegressionModel lrm = (LinearRegressionModel)tvsm.bestModel();
		
		System.out.println("Model Training R2="+lrm.summary().r2() + " RMSE = " + lrm.summary().rootMeanSquaredError());
		//PREDICT using test....
		
		//lrm.transform(testData).show();
		
		//Dont need to run transform and predict to run evaluate R2 rmse on test data
		System.out.println("Model TEST R2="+lrm.evaluate(holdoutData).r2() + " RMSE = " + lrm.evaluate(holdoutData).rootMeanSquaredError());
		
		 //coeffcs, intercept and params for bestmodel
		
		System.out.println("Coeffs:" + lrm.coefficients() + " intercept:" + lrm.intercept());
		System.out.println("reg param:" + lrm.getRegParam() + " elastic:" + lrm.getElasticNetParam());
		
		System.out.println("Time = " + (System.currentTimeMillis()-start));
		
	}

}
