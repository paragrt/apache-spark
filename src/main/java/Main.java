import java.util.ArrayList;
import java.util.List;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

public class Main {

	public static void main(String[] args) {
		List<Double> inputData = new ArrayList<>();
		/*for(int i = 0; i < 1000; i++)
		{
			inputData.add(Math.random() * 100);
		}*/
		inputData.add(5.5);
		inputData.add(3.5);
		inputData.add(35.5);
		inputData.add(85.5);
		inputData.add(12.499);
		inputData.add(90.32);
		inputData.add(20.32);
		
		Logger.getLogger("org.apache").setLevel(Level.WARN);
		
		//local[*] means use all cores all nodes
		//local=single threaded
		SparkConf conf = new SparkConf().setAppName("startingSpark").setMaster("local[*]");
		
		JavaSparkContext sc = new JavaSparkContext(conf);

		JavaRDD<Double> myRdd = sc.parallelize(inputData);
		
		Double result = myRdd.reduce( (Double v1, Double v2) -> v1+v2);
		System.out.println("Result = "+ result);
		
		JavaRDD<Double> sqrtRdd = myRdd.map((value) -> Math.sqrt(value) );
		
		//Multi CPU machines will throw NotSerializable error
		//so change 
		sqrtRdd.collect().forEach( (v) -> System.out.println("---"+v) );
		sqrtRdd.foreach( (v) -> System.out.println("***"+v) );
		
		
	}

}
