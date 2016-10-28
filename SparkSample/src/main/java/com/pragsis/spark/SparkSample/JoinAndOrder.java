package com.pragsis.spark.SparkSample;

import java.io.Serializable;
import java.util.Arrays;
import java.util.Comparator;

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
 * Ejemplo de Movielens con reduceByKey, join y orderByValue. 
 * 
 * Join entre el dataset de usuarios y ratings para obtener la ocupación de los usuarios, y luego un reduceByKey para
 * ver los ratings por ocupación. Por último ordenación descendente por el número de ratings.
 *
 */
public class JoinAndOrder {
	public static void main(String[] args) {
		
		SparkConf conf = new SparkConf().setAppName("WordCount").setMaster("local[*]");
		JavaSparkContext sc = new JavaSparkContext(conf);
		
		// Rating file
		JavaRDD<String> fileRatings = sc.textFile("/home/jfrancisco.vicente/datasets/ml-100k/u.data");
		JavaRDD<String> ratingRDD = fileRatings.flatMap(new FlatMapFunction<String, String>() {
			public Iterable<String> call(String s) {
				String[] userIds = s.split("\\t");
				return Arrays.asList(userIds[0]);
			}
		});

		// User file
		JavaRDD<String> fileUsers = sc.textFile("/home/jfrancisco.vicente/datasets/ml-100k/u.user");
		JavaRDD<Tuple2<String, String>> userRDD = fileUsers
				.flatMap(new FlatMapFunction<String, Tuple2<String, String>>() {

					public Iterable<Tuple2<String, String>> call(String t) throws Exception {
						String[] userFields = t.split("\\|");
						return Arrays.asList(new Tuple2<String, String>(userFields[0], userFields[3]));
					}

				});

		JavaPairRDD<String, Integer> pairs = ratingRDD.mapToPair(new PairFunction<String, String, Integer>() {
			public Tuple2<String, Integer> call(String s) {
				return new Tuple2<String, Integer>(s, 1);
			}
		});


		/// preparing RDDs before de join
		JavaPairRDD<String, Integer> ratings = pairs
				.mapToPair(new PairFunction<Tuple2<String, Integer>, String, Integer>() {
					public Tuple2<String, Integer> call(Tuple2<String, Integer> t) throws Exception {
						return new Tuple2<String, Integer>(t._1, t._2);
					}
				});

		JavaPairRDD<String, String> occupations = userRDD
				.mapToPair(new PairFunction<Tuple2<String, String>, String, String>() {
					public Tuple2<String, String> call(Tuple2<String, String> t) throws Exception {
						return new Tuple2<String, String>(t._1, t._2);
					}
				});

		// We make first the join, then we do a reduceByKey to get the accumulate ratings by occupations
		JavaPairRDD<String, Integer> occupationRatings = ratings.join(occupations).mapToPair(new PairFunction<Tuple2<String,Tuple2<Integer,String>>, String, Integer>() {

			public Tuple2<String, Integer> call(Tuple2<String, Tuple2<Integer, String>> t) throws Exception {
				
				return new Tuple2<String, Integer>(t._2._2, t._2._1);
			}

		}).reduceByKey(new Function2<Integer, Integer, Integer>() {
			public Integer call(Integer a, Integer b) {
				return a + b;
			}
		});

		// Then we swap key&value and then order by value
		JavaRDD<Tuple2<Integer, String>> orderedInvertedOccupationCounts = occupationRatings
				.map(new Function<Tuple2<String, Integer>, Tuple2<Integer, String>>() {

					public Tuple2<Integer, String> call(Tuple2<String, Integer> v1) throws Exception {
						return new Tuple2<Integer, String>(v1._2, v1._1);
					}
				}).sortBy(new Function<Tuple2<Integer, String>, Integer>() {

					public Integer call(Tuple2<Integer, String> v1) throws Exception {

						return v1._1;
					}

				}, false, 1);

		// And finally we swap again key&value to present the data
		JavaRDD<Tuple2<String, Integer>> orderedOccupationCounts = orderedInvertedOccupationCounts
				.map(new Function<Tuple2<Integer, String>, Tuple2<String, Integer>>() {

					public Tuple2<String, Integer> call(Tuple2<Integer, String> v1) throws Exception {

						return new Tuple2<String, Integer>(v1._2, v1._1);
					}

				});
		
		
		System.out.println(orderedOccupationCounts.take(10));
		orderedOccupationCounts.saveAsTextFile("/home/jfrancisco.vicente/datasets/resultados" + System.currentTimeMillis());
		sc.close();
	}

}

class TupleComparator implements Comparator<Tuple2<Integer, String>>, Serializable {
	public int compare(Tuple2<Integer, String> tuple1, Tuple2<Integer, String> tuple2) {
		return tuple1._1 < tuple2._1 ? 0 : 1;
	}
}
