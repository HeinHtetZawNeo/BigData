package cs523.SparkWC;

import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.DataFrame;
import org.apache.spark.sql.hive.HiveContext;

public class SparkHiveAnalysis {
    public static void main(String[] args) {
        // Set up Spark configuration
        SparkConf sparkConf = new SparkConf()
                .setAppName("SparkHiveAnalysis")
                .setMaster("local[*]") // Use "local[]" for local mode
                .set("spark.sql.catalogImplementation", "hive");

        HiveConf conf = new HiveConf();
        conf.set("hive.exec.scratchdir", "/user/cloudera/output");


        // Set up Spark context
        JavaSparkContext sparkContext = new JavaSparkContext(sparkConf);

        // Set up Hive context with Spark support
        HiveContext hiveContext = new HiveContext(sparkContext);

    
            // Read the "employees" table into a DataFrame
        	DataFrame top10EmployeesDF = hiveContext.sql("SELECT * FROM default.commodities ");
        	top10EmployeesDF.show();


            // Select the first 10 rows using DataFrame operations
            //DataFrame top10EmployeesDF = employeesDF.limit(10);

            // Show the results
           // top10EmployeesDF.show();
      
            // Stop the Spark context
            sparkContext.stop();
        }
    
}