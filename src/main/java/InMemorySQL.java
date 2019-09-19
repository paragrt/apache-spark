import java.util.ArrayList;
import java.util.List;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.DataType;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.Metadata;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;

public class InMemorySQL {

	public static void main(String[] args) {

		Logger.getLogger("org.apache").setLevel(Level.WARN);
		// TODO Auto-generated method stub
		String logFile = "/home/paragrt/Desktop/eclipse-wkspc/SparkTest/src/main/resources/students.csv";
		SparkSession spark = SparkSession.builder().appName("Simple Application").config("spark.master", "local[*]")
				.getOrCreate();
		
		List<Row> inMemory = new ArrayList<Row>();
		inMemory.add(RowFactory.create("WARN", "2016-12-31 04:19:32"));
		inMemory.add(RowFactory.create("FATAL","2016-12-31 03:22:34"));
		inMemory.add(RowFactory.create("WARN", "2016-12-31 03:21:21"));
		inMemory.add(RowFactory.create("INFO", "2015-04-21 14:32:21"));
		inMemory.add(RowFactory.create("FATAL","2015-04-21 19:23:20"));
		
		
		StructField[] fields = new StructField[] {
				new StructField("level",DataTypes.StringType, false, Metadata.empty()),
				new StructField("datetime",DataTypes.StringType, false, Metadata.empty()),
		};
		StructType schema = new StructType(fields);
		Dataset<Row> rows = spark.createDataFrame(inMemory, schema );
		
		rows.createOrReplaceTempView("mylog");
		//unique to spark
		spark.sql("select level, collect_list(datetime) from mylog group by level").show();
		//similar to traditional RDBMS
		spark.sql("select level, count(datetime) from mylog group by level").show();
		spark.sql("select level, date_format(datetime, 'MMMM') as month, "
				+ "count(1) from mylog group by level, month ").show();
		//rows.show();
		
		spark.stop();
	}

};