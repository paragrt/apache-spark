import java.util.ArrayList;
import java.util.List;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

import scala.Tuple2;

public class PairRDD {

	public static void main(String[] args) {
		List<String> inputData = new ArrayList<>();
		/*for(int i = 0; i < 1000; i++)
		{
			inputData.add(Math.random() * 100);
		}*/
		inputData.add("WARN: Tuesday 4 Sept 2019");
		inputData.add("WARN: Wed 5 Sept 2019");
		inputData.add("ERROR: Tuesday 4 Sept 2019");
		inputData.add("WARN: Thur 7 Sept 2019");
		inputData.add("WARN: Tuesday 4 Sept 2019");
		inputData.add("DEBUG: Tuesday 4 Sept 2019");
		inputData.add("WARN: Tuesday 4 Sept 2019");
		inputData.add("TRACE: Tuesday 11 Sept 2019");
		inputData.add("WARN: Tuesday 18 Sept 2019");
		inputData.add("WARN: Tuesday 25 Sept 2019");
		inputData.add("INFO: Wed 12 Sept 2019");
		inputData.add("WARN: Tuesday 4 Sept 2019");
		inputData.add("WARN: Tuesday 4 Sept 2019");
		inputData.add("DEBUG: Tuesday 4 Sept 2019");
		inputData.add("WARN: Tuesday 4 Sept 2019");
		inputData.add("TRACE: Tuesday 4 Sept 2019");
		inputData.add("WARN: Tuesday 4 Sept 2019");
		inputData.add("ERROR: Tuesday 4 Sept 2019");
		
		Logger.getLogger("org.apache").setLevel(Level.WARN);
		
		//local[*] means use all cores all nodes
		//local=single threaded
		SparkConf conf = new SparkConf().setAppName("startingSpark").setMaster("local[*]");
		
		JavaSparkContext sc = new JavaSparkContext(conf);

		JavaRDD<String> myRdd = sc.parallelize(inputData);
		JavaPairRDD<String, String> myRddPair = myRdd.mapToPair( (String s) -> {
			String[] columns = s.split(":");
			return new Tuple2<String, String>(columns[0], columns[1]);
		});
		
		
		myRddPair.collect().forEach( (v) -> System.out.println("---"+v) );
		
		//using group by returns iterable and causes memory crashes if the values list i s large
		
		//we use reducebykey to count the nbr of warns, debugs, infos etc
		
		JavaPairRDD<String, Integer> myRddPairInt = myRdd.mapToPair( (String s) -> {
			String[] columns = s.split(":");
			return new Tuple2<String, Integer>(columns[0], 1);
		});
		
		JavaPairRDD<String, Integer> resultRdd = myRddPairInt.reduceByKey( ( Integer v1, Integer v2) -> v1+v2 );
		resultRdd.collect().forEach( (v) -> System.out.println("---"+v) );
	}

}
