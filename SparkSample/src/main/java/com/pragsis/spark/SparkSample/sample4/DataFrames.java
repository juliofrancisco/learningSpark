package com.pragsis.spark.SparkSample.sample4;

import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.sql.DataFrame;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SQLContext;

import scala.Tuple2;


/**
 * @author jfrancisco.vicente
 *
 * Ejemplo de Movielens desde BD Posgres y fichero de texto
 * 
 */
public class DataFrames {
	public static void main(String[] args) {
		
		String userFile = "";
		String outDir = "";
		
		
		if (args.length != 2) {		
			return;
		} else {
			userFile = args[0];
			outDir = args[1];			
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
		
		// Recover data from POSTGRE
		JavaRDD<Row> userRatings = sqlContext.sql("SELECT user_id, rating FROM data").toJavaRDD();		
		JavaPairRDD<String,Integer> userRatingPairRdd = userRatings.mapToPair(new PairFunction<Row, String, Integer>() {

			@Override
			public Tuple2<String, Integer> call(Row arg0) throws Exception {
				
				return new Tuple2<String,Integer>(arg0.getString(0), 1);
			}
		});

		// Recover data from TEXT file
		JavaRDD<String> fileUsers = sc.textFile(userFile);
		JavaRDD<Tuple2<String, String>> userRDD = fileUsers
				.flatMap(new FlatMapFunction<String, Tuple2<String, String>>() {
					public Iterable<Tuple2<String, String>> call(String t) throws Exception {
						String[] userFields = t.split("\\|");
						return Arrays.asList(new Tuple2<String, String>(userFields[0], userFields[3]));
					}
				});
		
		JavaPairRDD<String,String> userRDDPairRdd = userRDD.mapToPair(new PairFunction<Tuple2<String,String>, String, String>() {
			@Override
			public Tuple2<String, String> call(Tuple2<String, String> arg0) throws Exception {
				return new Tuple2<String,String>(arg0._1,arg0._2);
			}
		});

		
		// Join both sources, reduceByKey and sort
		JavaRDD<Tuple2<String, Integer>> joinedPairRDD = userRatingPairRdd.join(userRDDPairRdd).mapToPair(new PairFunction<Tuple2<String,Tuple2<Integer,String>>, String, Integer>() {
			public Tuple2<String, Integer> call(Tuple2<String, Tuple2<Integer, String>> t) throws Exception {
				return new Tuple2<String, Integer>(t._2._2, t._2._1);
			}
		}).reduceByKey(new Function2<Integer, Integer, Integer>() {
			public Integer call(Integer v1, Integer v2) throws Exception {
				return v1+v2;
			}
		}).map(new Function<Tuple2<String,Integer>, Tuple2<Integer,String>>() {

			@Override
			public Tuple2<Integer, String> call(Tuple2<String, Integer> v1) throws Exception {
				return new Tuple2<Integer,String>(v1._2,v1._1);
			}
		}).sortBy(new Function<Tuple2<Integer,String>, Integer>() {
			public Integer call(Tuple2<Integer, String> v1) throws Exception {
				return v1._1;
			}
		}, false, 1).map(new Function<Tuple2<Integer,String>, Tuple2<String, Integer>>() {
			public Tuple2<String, Integer> call(Tuple2<Integer, String> v1) throws Exception {
				
				return new Tuple2<String, Integer>(v1._2,v1._1);
			}
		});
		
		System.out.println(joinedPairRDD.take(10));

		joinedPairRDD.saveAsTextFile(outDir+ System.currentTimeMillis() + "_DataFrames2");

		sc.close();
		
		
	}

}
