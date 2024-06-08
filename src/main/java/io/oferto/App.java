package io.oferto;

import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;

public class App 
{
    public static void main( String[] args ) {          
        SparkSession sparkSession = SparkSession
	        .builder()
		        .appName("Minio SQL App")
			        .config("fs.s3a.access.key", "admin")
			        .config("fs.s3a.secret.key", "password")
			        .config("fs.s3a.endpoint", "localhost:9010")
			        .config("fs.s3a.connection.ssl.enabled", "false")
			        .config("fs.s3a.path.style.access", "true")
			        .config("fs.s3a.attempts.maximum", "1")
			        .config("fs.s3a.connection.establish.timeout", "5000")
			        .config("fs.s3a.connection.timeout", "10000")
		        .master("local[*]")
	        .getOrCreate();
        
        Dataset<Row> ds = sparkSession.read().format("csv").option("header", "true").load("s3a://samples/addresses.csv");
        
        System.out.println("CSV Data: " + ds.count());
        ds.foreach(row -> {
        	System.out.println(row.toString());
        }); 
        
        Dataset<Row> dsFiltered = ds.select("name", "surname"); 
        
        System.out.println("CSV Data: " + dsFiltered.count());
        dsFiltered.foreach(row -> {
        	System.out.println(row.toString());
        }); 
    }
}
