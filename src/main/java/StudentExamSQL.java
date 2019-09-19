import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

public class StudentExamSQL {

	public static void main(String[] args) {

		Logger.getLogger("org.apache").setLevel(Level.WARN);
		// TODO Auto-generated method stub
		String logFile = "/home/paragrt/Desktop/eclipse-wkspc/SparkTest/src/main/resources/students.csv";
		SparkSession spark = SparkSession.builder().appName("Simple Application").config("spark.master", "local[*]")
				.getOrCreate();
		Dataset<Row> rows = spark.read().option("header", true).csv(logFile).cache();
		//rows.show();
		//System.out.println("Rows=" + rows.count());

		/*
		Dataset<Row> modArt = rows.filter("subject != 'Modern Art' "
				+ "and (student_id > 10 and student_id < 100) "
				+ "and (grade = 'A' or year > 2006)");
		modArt.show();
		*/
		rows.createOrReplaceTempView("my_s");
		
		spark.sql("select max(score) from my_s where subject = 'French'").show();
		
		spark.sql("select distinct(year) from my_s order by year desc").show();
		
		spark.stop();
	}

};