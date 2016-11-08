package com.pragsis.spark.SparkSample.sample6;

import java.io.ByteArrayOutputStream;
import java.io.ObjectOutputStream;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.spark.JavaHBaseContext;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;

import scala.Tuple2;


/**
 * @author jfrancisco.vicente
 *
 * 
 */
public class HBase {
	public static void main(String[] args) {
		
		String userFile = "";
				
		if (args.length != 1) {		
			return;
		} else {
			userFile = args[0];	
		}
				
		SparkConf conf = new SparkConf().setAppName("HBase").setMaster("local[*]");
		JavaSparkContext sc = new JavaSparkContext(conf);
		
		Configuration hbaseConf = HBaseConfiguration.create();
//		hbaseConf.setInt("timeout", 120);
//		hbaseConf.set("hbase.master", "*" + "192.168.2.201" + ":9000*");
//		hbaseConf.set("hbase.zookeeper.quorum","192.168.2.201");
//		hbaseConf.set("hbase.zookeeper.property.clientPort", "2181");
		JavaHBaseContext hbaseContext = new JavaHBaseContext(sc, hbaseConf);
	
		JavaRDD<String> textFile = sc.textFile(userFile);
		
		JavaRDD<String> fields = textFile.filter(new Function<String, Boolean>() {

			@Override
			public Boolean call(String row) throws Exception {
				return !row.startsWith("CodigoCliente");
			}
		});
				
		JavaRDD<Tuple2<Integer,Client>> client = fields.map(new Function<String, Tuple2<Integer,Client>>() {

			@Override
			public Tuple2<Integer, Client> call(String arg0) throws Exception {
				String[] splitFields = arg0.split("\\t");
				Integer cliendId = new Integer(splitFields[0]);
				Client client = new Client();
				client.setIdCliente(Integer.parseInt(splitFields[0]));
				client.setResidencia(splitFields[1]);
				client.setCodigoOperacion(splitFields[2]);
				return new Tuple2<Integer,Client>(cliendId,client);
			}
		});
				
		hbaseContext.bulkPut(client, TableName.valueOf("cliente"), new PutFunction());
		
		sc.close();
		
		
	}
	
    public static class PutFunction implements Function<Tuple2<Integer,Client>, Put> {

		private static final long serialVersionUID = 1L;

		public Put call(Tuple2<Integer, Client> arg0) throws Exception {
	        
	        Put put = new Put(Bytes.toBytes(arg0._1()));
	  
	        ByteArrayOutputStream bs= new ByteArrayOutputStream();
	        ObjectOutputStream os = new ObjectOutputStream (bs);
	        os.writeObject(arg0._2()); 
	        os.close();
	        byte[] clientData =  bs.toByteArray(); 
	        
	        put.addColumn("data".getBytes(), "cliente".getBytes(), clientData);
	        
	        return put;
		} 
	
	}
}
