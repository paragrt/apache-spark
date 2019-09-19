import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.api.java.function.FilterFunction;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.SparkSession;

public class SimpleApp {

	public static void main(String[] args) {
		
		Logger.getLogger("org.apache").setLevel(Level.WARN);
		// TODO Auto-generated method stub
		String logFile = "/home/paragrt/Desktop/spark-2.4.3-bin-hadoop2.7/README.md";
		SparkSession spark = SparkSession.builder().appName("Simple Application")
				.config("spark.master", "local[*]")
				.getOrCreate();
	    Dataset<String> logData = spark.read().textFile(logFile).cache();

	    //2 ways to define the Functions
	    FilterFunction<String> f1 = s -> s.contains("a");
	    long numAs = logData.filter(f1).count();
	    long numBs = logData.filter( (String s) -> s.contains("b") ).count();
	    long numNotCs = logData.filter( (String s) -> !s.contains("c") ).count();

	    System.out.println("Lines with a: " + numAs + ", lines with b: " + numBs
	    		+" lines without c:" + numNotCs);

	    spark.stop();
	}

}
;