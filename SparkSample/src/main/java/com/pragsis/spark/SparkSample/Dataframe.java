package com.pragsis.spark.SparkSample;

import java.util.Arrays;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;

import scala.Tuple2;


/**
 * @author jfrancisco.vicente
 *
 * Ejemplo de Movielens con Dataframes 
 * 
 *
 */
public class Dataframe {
	public static void main(String[] args) {
		
		SparkConf conf = new SparkConf().setAppName("Dataframes").setMaster("local[*]");
		JavaSparkContext sc = new JavaSparkContext(conf);
		
		
		
		sc.close();
	}

}
