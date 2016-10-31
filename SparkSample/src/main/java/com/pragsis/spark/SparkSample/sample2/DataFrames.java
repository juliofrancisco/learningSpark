package com.pragsis.spark.SparkSample.sample2;

import java.util.HashMap;
import java.util.Map;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.DataFrame;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SQLContext;


/**
 * @author jfrancisco.vicente
 *
 * Ejemplo de Movielens SparkQL
 * 
 */
public class DataFrames {
	public static void main(String[] args) {
		
		String outDir = "";
		
		
		if (args.length != 1) {		
			return;
		} else {
			outDir = args[0];	
		}
		
		
		SparkConf conf = new SparkConf().setAppName("DataFrames").setMaster("local[*]");
		JavaSparkContext sc = new JavaSparkContext(conf);
		SQLContext sqlContext = new org.apache.spark.sql.SQLContext(sc);
		
		
		Map<String, String> options = new HashMap<String, String>();
		options.put("url", "jdbc:postgresql://192.168.2.199/julio");
		options.put("dbtable", "data");
	    options.put("user", "postgres");
	    options.put("password", "postgres");
	    options.put("driver", "org.postgresql.Driver");
	    
	    // Create the dataframe from DATA Postgres TABLE
		DataFrame dfData = sqlContext.read().format("jdbc").options(options).load();
		dfData.registerTempTable("data");
		
		options.remove("dtable");
		options.put("dbtable", "userinfo");
		// Create the dataframe from USERINFO Postgres TABLE
		DataFrame dfUser = sqlContext.read().format("jdbc").options(options).load();
		dfUser.registerTempTable("userinfo");

		
		// Join and agreggation
		JavaRDD<Row> userRatings = sqlContext.sql(" select agregate.occupation, count(*) as rating from "
				+ "									(SELECT user.occupation, data.rating "
				+ "										FROM data as data, userinfo as user "
				+ "										WHERE data.user_id = user.user_id) as agregate"
				+ "									 group by agregate.occupation order by rating desc").toJavaRDD();
		
				
		userRatings.saveAsTextFile(outDir+ System.currentTimeMillis() + "_DataFrames");

		sc.close();
		
		
	}

}
