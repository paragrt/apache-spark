import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.sql.Column;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.functions;
import org.apache.spark.sql.types.DataTypes;
//Test
public class BigLog {

	public static void main(String[] args) {

		Logger.getLogger("org.apache").setLevel(Level.WARN);
		// TODO Auto-generated method stub
		String logFile = "/home/paragrt/Desktop/eclipse-wkspc/SparkTest/src/main/resources/biglog.txt";
		SparkSession spark = SparkSession.builder().appName("Simple Application").config("spark.master", "local[*]")
				.getOrCreate();
		
		Dataset<Row> rows = spark.read().option("header", true).csv(logFile).cache();
			
		rows.createOrReplaceTempView("mylog");
		
		/*
		spark.sql("select level, date_format(datetime, 'MMMM') as month, "
				+ "cast(first(date_format(datetime, 'M')) as int) as num_month, count(1) as total "
				+ "from mylog group by level, month order by num_month");
		
		Dataset<Row> dataset = spark.sql("select level, date_format(datetime, 'MMMM') as month, "
				+ " count(1) as total "
				+ "from mylog group by level, month "
				+ "order by cast(first(date_format(datetime, 'M')) as int), level");
        */
		//pivot tables
		rows = rows.select(functions.col("level"),
				functions.date_format(functions.col("datetime"), "MMMM").alias("month"),
				functions.date_format(functions.col("datetime"), "M").alias("monthnum").cast(DataTypes.IntegerType)
				);
		Dataset<Row> pivoted_dataset = rows.groupBy("monthnum").pivot("level").count();
		
		pivoted_dataset.orderBy("monthnum").show();
		//unique to spark
		//System.out.println(rows.count());
		//rows.show();
		
		spark.stop();
	}

};