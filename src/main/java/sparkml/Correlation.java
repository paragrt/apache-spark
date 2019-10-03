package sparkml;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.ml.evaluation.RegressionEvaluator;
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

public class Correlation {

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
		
		dataset = dataset.drop("date");
		//Correlation Matrix
		
		System.out.print("Properties,");
		for (String col : dataset.columns()) {		
			System.out.print(col + ",");
		}
		
		for (String col1 : dataset.columns()) {
			System.out.print("\n"+col1+",");
			for (String col2 : dataset.columns()) {		
				System.out.print(String.format("%.2f", dataset.stat().corr(col1, col2)) + ",");
			}			
		}
		
		
		System.out.println("Time = " + (System.currentTimeMillis()-start));
	}

}
