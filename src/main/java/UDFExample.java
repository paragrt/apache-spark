import static org.apache.spark.sql.functions.udf;

import java.util.HashMap;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.DataTypes;

public class UDFExample {

	public static void main(String[] args) {
		// TODO Auto-generated method stub
		Logger.getLogger("org.apache").setLevel(Level.WARN);
		// TODO Auto-generated method stub
		String logFile = "src/main/resources/biglog.txt";
		SparkSession spark = SparkSession.builder().appName("Simple Application").config("spark.master", "local[*]")
				.getOrCreate();
		Dataset<Row> rows = spark.read().option("header", true).csv(logFile).cache();
		
		HashMap<String, Integer> map = new HashMap<String, Integer>();
		map.put("January",  1);
		map.put("February",  2);
		map.put("March",  3);
		map.put("April",  4);
		map.put("May",  5);
		map.put("June",  6);
		map.put("July",  7);
		map.put("August",  8);
		map.put("September",  9);
		map.put("October",  10);
		map.put("November",  11);
		map.put("December",  12);
		
		//register udf
		spark.udf().register("monthnum", 
				(String str) -> {
					return map.get(str);
				}
				,
				DataTypes.IntegerType
			);
		
		rows.createOrReplaceTempView("logging_table");
		
		//pivot table with aggregate methods avg and stddev
		Dataset<Row> results = spark.sql("select level, date_format(datetime, 'MMMM') as month, count(1) as total "
				+ " from logging_table group by level, month order by monthnum(month), level");
		results.explain(true);
		results.show(100);
	}

}
