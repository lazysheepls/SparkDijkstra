import java.io.File;
import java.util.Arrays;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.commons.io.FileUtils;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

import scala.Tuple2;

public class AssigTwoz3451444 {

	public static void main(String[] args) throws Exception{
		SparkConf conf = new SparkConf().setMaster("local").setAppName("Ass2");
		
		// Process input arguments
		String init_start_node = args[0];
		String input_path = args[1];
		String output_path = args[2];
		
		// Create RDD from file
		JavaSparkContext context = new JavaSparkContext(conf);
		JavaRDD<String> inputRDD = context.textFile(input_path);

		//Delete old folders if exist
		File temp_folder_to_delete = new File("TEMP");
		if(temp_folder_to_delete.exists()) {
			FileUtils.cleanDirectory(temp_folder_to_delete);
			FileUtils.deleteDirectory(temp_folder_to_delete);
		}
		
		File output_folder_to_delete = new File("output");
		if(output_folder_to_delete.exists()) {
			FileUtils.cleanDirectory(output_folder_to_delete);
			FileUtils.deleteDirectory(output_folder_to_delete);
		}
		
		//DEBUG Remove warning info
		Logger.getLogger("org").setLevel(Level.OFF);
		Logger.getLogger("INFO").setLevel(Level.OFF);
		
		//DEBUG Print Raw inputs
		System.out.println("Start node: " + init_start_node);
		
		// Transformation: input RDD to a list of string
		JavaPairRDD<String,Tuple2<String,Integer>> inputPairs = inputRDD.mapToPair(line -> {
			String[] items = line.split(",");
			Tuple2<String, Integer> destAndDistPair = new Tuple2<String, Integer>(items[1], Integer.parseInt(items[2]));
			return new Tuple2<String,Tuple2<String,Integer>>(items[0],destAndDistPair);
		});
		
		//DEBUG Print input pair
		System.out.println("Pairs after first transformation.");
		inputPairs.collect().forEach(System.out::println);
	}

}
