import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import static org.apache.spark.sql.functions.*;
import org.apache.spark.sql.types.DataTypes;

/**
 * Updated the runtime stats.
 * @author paragrt
 *
 *The KEY difference is the HashAggregate vs the SortAggregate
 *When the group by contains strings, SQL is forced to use SortAggregate making it slower
 *Look out for the Cast that converts the monthnum in the select clause to a number
 *
SQL MODE Time=11656
== Physical Plan ==
*(3) Sort [monthnum#27 ASC NULLS FIRST, level#10 ASC NULLS FIRST], true, 0
+- Exchange rangepartitioning(monthnum#27 ASC NULLS FIRST, level#10 ASC NULLS FIRST, 200)
   +- *(2) HashAggregate(keys=[level#10, date_format(cast(datetime#11 as timestamp), MMMM, Some(America/Chicago))#82], functions=[count(1), first(cast(date_format(cast(datetime#11 as timestamp), M, Some(America/Chicago)) as int), false)])
      +- Exchange hashpartitioning(level#10, date_format(cast(datetime#11 as timestamp), MMMM, Some(America/Chicago))#82, 200)
         +- *(1) HashAggregate(keys=[level#10, date_format(cast(datetime#11 as timestamp), MMMM, Some(America/Chicago)) AS date_format(cast(datetime#11 as timestamp), MMMM, Some(America/Chicago))#82], functions=[partial_count(1), partial_first(cast(date_format(cast(datetime#11 as timestamp), M, Some(America/Chicago)) as int), false)])
            +- InMemoryTableScan [level#10, datetime#11]
                  +- InMemoryRelation [level#10, datetime#11], StorageLevel(disk, memory, deserialized, 1 replicas)
                        +- *(1) FileScan csv [level#10,datetime#11] Batched: false, Format: CSV, Location: InMemoryFileIndex[file:/home/paragrt/Desktop/github-repo/SparkTest/src/main/resources/biglog.txt], PartitionFilters: [], PushedFilters: [], ReadSchema: struct<level:string,datetime:string>

JAVA API MODE Time=12973
== Physical Plan ==
*(3) Project [level#10, month#24, count#34L]
+- *(3) Sort [monthnum#26 ASC NULLS FIRST], true, 0
   +- Exchange rangepartitioning(monthnum#26 ASC NULLS FIRST, 200)
      +- *(2) HashAggregate(keys=[level#10, month#24, monthnum#26], functions=[count(1)])
         +- Exchange hashpartitioning(level#10, month#24, monthnum#26, 200)
            +- *(1) HashAggregate(keys=[level#10, month#24, monthnum#26], functions=[partial_count(1)])
               +- *(1) Project [level#10, date_format(cast(datetime#11 as timestamp), MMMM, Some(America/Chicago)) AS month#24, cast(date_format(cast(datetime#11 as timestamp), M, Some(America/Chicago)) as int) AS monthnum#26]
                  +- InMemoryTableScan [datetime#11, level#10]
                        +- InMemoryRelation [level#10, datetime#11], StorageLevel(disk, memory, deserialized, 1 replicas)
                              +- *(1) FileScan csv [level#10,datetime#11] Batched: false, Format: CSV, Location: InMemoryFileIndex[file:/home/paragrt/Desktop/github-repo/SparkTest/src/main/resources/biglog.txt], PartitionFilters: [], PushedFilters: [], ReadSchema: struct<level:string,datetime:string>

 */
public class SQlVsJava2 {

	public static boolean SQLMODE = false;//toggle this to false to run JAVA mode

	public static void main(String[] args) {
		long start = System.currentTimeMillis();
		Logger.getLogger("org.apache").setLevel(Level.WARN);
		// TODO Auto-generated method stub
		String logFile = "src/main/resources/biglog.txt";
		SparkSession spark = SparkSession.builder().appName("Simple Application").config("spark.master", "local[*]")
				.getOrCreate();
		Dataset<Row> dataset = spark.read().option("header", true).csv(logFile).cache();

		dataset.createOrReplaceTempView("logging_table");
		Dataset<Row> results = null;
		if (SQLMODE) {
			results = spark.sql(
					  "select level, date_format(datetime, 'MMMM') as month, "
					+ "count(1) as total, "
					+ "first(cast(date_format(datetime, 'M') as int) ) as monthnum "
					+ "from logging_table group by level, month "
					+ "order by monthnum, level");

			results.show(100);
			System.out.println("SQL MODE Time=" + (System.currentTimeMillis() - start));
		} else {

			results = dataset.select(col("level"), date_format(col("datetime"), "MMMM").alias("month"),
					date_format(col("datetime"), "M").alias("monthnum").cast(DataTypes.IntegerType));
			results = results.groupBy("level", "month", "monthnum").count().as("total").orderBy("monthnum");
			results = results.drop("monthnum");
			results.show(100);
			
			System.out.println("JAVA API MODE Time=" + (System.currentTimeMillis() - start));
		}
		results.explain();
	}

}

