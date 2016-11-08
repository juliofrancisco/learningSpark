package com.pragsis.spark.SparkSample.sample5;

import java.util.Iterator;
import java.util.regex.Pattern;

import org.apache.log4j.Logger;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.broadcast.Broadcast;
import org.dom4j.DocumentException;

import com.modellica.engine.server.machine.SparkMachine;

public class ModellicaTest {
	
	private static final Logger log = Logger.getLogger(ModellicaTest.class);
	private static final String delimiter = "\t";
	private static final Pattern tab = Pattern.compile(Pattern.quote(delimiter));

	public static void main(String[] args) {
		
		log.info("Starting up");
		
		if (args.length < 1) {
			log.error("Usage: ModellicaTest <xmlFilePath>");
			System.exit(1);
		}
		
		SparkConf conf = new SparkConf();
		JavaSparkContext sc = new JavaSparkContext(conf);
		SparkMachine sparkMachine = null;
		try {
			sparkMachine = new SparkMachine(args[0]);
		} catch (DocumentException e) {
			log.warn("Error parsing xml file: DocumentException", e);
		} catch (Exception e) {
			log.error("Generic exception while instantiating SparkMachine", e);
		}
		
		log.info("XML file read, Modellica SparkMachine instantiated");
		
		String inputFile = sparkMachine.getInputFile(), outputFolder = sparkMachine.getOutputFolder();
		final Broadcast<SparkMachine> bcMachine = sc.broadcast(sparkMachine);
		
		JavaRDD<String> input = sc.textFile(inputFile);
		final String[] headers = tab.split(input.first());
		
		log.info("SparkMachine bc'ed, input file read");
		
		input = input.mapPartitionsWithIndex(new Function2<Integer, Iterator<String>, Iterator<String>>() {
			@Override
			public Iterator<String> call(Integer i, Iterator<String> it) throws Exception {
				if (i == 0 && it.hasNext()) {
					it.next();
				}
				return it;
			}
		}, false);
		
		log.info("Input header retrieved and removed from RDD");
		
		JavaRDD<String> output = input.map(new Function<String, String>() {
			@Override
			public String call(String in) throws Exception {
				SparkMachine sMachine = bcMachine.getValue();
				sMachine.initializeDelimitedReaderAndWriter(headers, delimiter);
				return sMachine.processRequest(in);
			}
		});
		
		output.saveAsTextFile(outputFolder);
		
		sc.stop();
		sc.close();
	}
}
