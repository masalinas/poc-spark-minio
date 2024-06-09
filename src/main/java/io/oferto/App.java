package io.oferto;

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.HashMap;
import java.util.Map;

import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

public class App 
{
    public static void main( String[] args ) throws IllegalArgumentException, IOException, URISyntaxException {          
        SparkSession sparkSession = SparkSession
	        .builder()
		        .appName("Minio SQL App")
		        	.config("fs.s3a.connection.ssl.enabled", "false")
			        .config("fs.s3a.endpoint", "localhost:9010")
			        .config("fs.s3a.access.key", "admin")
			        .config("fs.s3a.secret.key", "password")
			        .config("fs.s3a.path.style.access", "true")
			        .config("fs.s3a.attempts.maximum", "1")
			        .config("fs.s3a.connection.establish.timeout", "5000")
			        .config("fs.s3a.connection.timeout", "10000")
		        .master("local")
	        .getOrCreate();
        
        System.out.println("Sample 01: get all CSV rows ...");
        System.out.println("");
        
        // Load csv resource from minio
        Map<String, String> optionsMap = new HashMap<String, String>();
        optionsMap.put("delimiter", ",");
        optionsMap.put("header", "true");
        
        //Dataset<Row> ds = sparkSession.read().format("csv").option("header", "true").load("s3a://samples/addresses.csv");
        Dataset<Row> ds = sparkSession.read().options(optionsMap).csv("s3a://samples/addresses.csv");
        
        // Get all Dataframe
        System.out.println("CSV Rows: " + ds.count());
        
        ds.foreach(row -> {
        	System.out.println(row.toString());
        }); 
                        
        System.out.println("Sample 02: filter CSV rows ...");
        System.out.println("");
        
        // Select Dataframe
        Dataset<Row> dsFiltered = ds.select("name", "surname"); 
        
        System.out.println("CSV Rows: " + dsFiltered.count());
        dsFiltered.foreach(row -> {
        	System.out.println(row.toString());
        }); 
        
        System.out.println("Sample 03: SQL CSV rows ...");
        System.out.println("");
        
        // Cache the DataFrame
        ds.cache();

        // Create a temporary view of the DataFrame
        ds.createOrReplaceTempView("table");
        
        // SQL Dataframe
        Dataset<Row> dsSQL= sparkSession.sql("""
        		  SELECT concat(name, ' ', surname) as name,
        		         address
        		  FROM table
        		  WHERE name = 'John'
        		""");
        
        System.out.println("CSV Rows: " + dsSQL.count());
        dsSQL.foreach(row -> {
        	System.out.println(row.toString());
        }); 
        
        System.out.println("Sample 04: save CSV dataset ...");
        System.out.println("");
        
        dsSQL.write().format("csv").options(optionsMap).mode("Overwrite").save("s3a://samples/addresses_filtered.csv");
        
        FileSystem fileSystem = FileSystem.get(new URI("s3a://samples"), sparkSession.sparkContext().hadoopConfiguration());
        String fileName = fileSystem.globStatus(new Path("/addresses_filtered.csv/part*"))[0].getPath().getName();
        		
        fileSystem.delete(new Path("/addresses_filtered_final.csv"), true);
        
        fileSystem.rename(
        		new Path("/addresses_filtered.csv/" + fileName), 
        		new Path("/addresses_filtered_final.csv"));
        
        fileSystem.delete(new Path("/addresses_filtered.csv"), true);
    }
}
