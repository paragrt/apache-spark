import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import static org.apache.spark.sql.functions.*;

public class PivotTable {

	public static void main(String[] args) {
		// TODO Auto-generated method stub
		Logger.getLogger("org.apache").setLevel(Level.WARN);
		// TODO Auto-generated method stub
		String logFile = "src/main/resources/students.csv";
		SparkSession spark = SparkSession.builder().appName("Simple Application").config("spark.master", "local[*]")
				.getOrCreate();
		Dataset<Row> rows = spark.read().option("header", true).csv(logFile).cache();
		
		//pivot table with aggregate methods avg and stddev
		rows = rows.groupBy("subject").pivot("year").agg( 
				round(avg(col("score")), 2).alias("avg") ,
				round(stddev(col("score"))).alias("stddev") 
				);
		rows.show();
	}

}
