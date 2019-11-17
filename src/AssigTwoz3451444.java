import java.io.File;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.commons.io.FileUtils;
import org.apache.hadoop.fs.Path;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

import scala.Tuple2;
import scala.Tuple3;

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
		
		// Transformation 1: input RDD to a list of string
		JavaPairRDD<String,Tuple2<String,Integer>> inputPairs = inputRDD.mapToPair(line -> {
			String[] items = line.split(",");
			Tuple2<String, Integer> destAndDistPair = new Tuple2<String, Integer>(items[1], Integer.parseInt(items[2]));
			return new Tuple2<String,Tuple2<String,Integer>>(items[0],destAndDistPair);
		});
		
		// Get total number of nodes and their names
		File tempFile = new File("TEMP/temp.txt");
		FileUtils.touch(tempFile);
		inputPairs.foreach(pair -> {
			String curNode = pair._1;
			String destNode = pair._2._1;
			List<String> nodeNames = Arrays.asList(FileUtils.readFileToString(tempFile).split(","));
			if (!nodeNames.contains(curNode)) {
				if (tempFile.length()!=0) 
					FileUtils.writeStringToFile(tempFile,",",true);
				FileUtils.writeStringToFile(tempFile,curNode,true);
			} 
				
			if (curNode != destNode && !nodeNames.contains(destNode)) {
				if (tempFile.length()!=0) 
					FileUtils.writeStringToFile(tempFile,",",true);
				FileUtils.writeStringToFile(tempFile,destNode,true);
			}
		});
		int numberOfNodes = Arrays.asList(FileUtils.readFileToString(tempFile).split(",")).size();
		
		//DEBUG Print input pair
		System.out.println("Total number of nodes: " + Integer.toString(numberOfNodes));
		System.out.println("Pairs after first transformation.");
		inputPairs.collect().forEach(System.out::println);
		
		// Action: Group input by current nodes
		JavaPairRDD<String,Iterable<Tuple2<String,Integer>>> inputWithAdjList = inputPairs.groupByKey();
		
		//DEBUG Print group outputs
		System.out.println("Group input pairs by key.");
		inputWithAdjList.collect().forEach(System.out::println);
		
		// Transformation: Add path and distance to start node
		JavaPairRDD<String,Tuple3<Integer,Iterable<String>,Iterable<Tuple2<String,Integer>>>> inputWithPathAndAdjList = 
				inputWithAdjList.mapToPair(item -> {
					String curNode = item._1;
					int distFromStartToCurNode = Integer.MAX_VALUE;
					if (init_start_node.equals(curNode))
						distFromStartToCurNode = 0;
					Iterable<String> path = Arrays.asList(curNode);
					Iterable<Tuple2<String,Integer>> adjacentList = item._2;
					return new Tuple2<String,Tuple3<Integer,Iterable<String>,Iterable<Tuple2<String,Integer>>>>(
							curNode,new Tuple3<Integer,Iterable<String>,Iterable<Tuple2<String,Integer>>>(
									distFromStartToCurNode,path,adjacentList));
				});
		
		//DEBUG Print final inputs
		System.out.println("Final inputs with path and adj list");
		inputWithPathAndAdjList.collect().forEach(System.out::println);
		
		// Transformation: Emit
		JavaPairRDD<String,Tuple3<Integer,Iterable<String>,Iterable<Tuple2<String,Integer>>>> emittedPairs =
				inputWithPathAndAdjList.flatMapToPair(item -> {
					// Add current item to the emitted pairs list
					List<Tuple2<String,Tuple3<Integer,Iterable<String>,Iterable<Tuple2<String,Integer>>>>> localEmittedPairs =
						new ArrayList<Tuple2<String,Tuple3<Integer,Iterable<String>,Iterable<Tuple2<String,Integer>>>>>();
					localEmittedPairs.add(item);
					// Iterate through adjacent nodes
					String curNode = item._1;
					int curDist = item._2._1();
					Iterable<String> curPath = item._2._2();
					Iterable<Tuple2<String,Integer>> adjList = item._2._3();
					for(Tuple2<String,Integer> adjItem:adjList) {
						String adjNode = adjItem._1;
						int adjDist = adjItem._2;
						int updatedDist = Integer.MAX_VALUE;
						List<String> updatedPath = new ArrayList<String>();
						// Update distance
						if (curDist != Integer.MAX_VALUE) {
							updatedDist = curDist + adjDist;
						}
						// Update path
						Iterator<String> curPathIterator = curPath.iterator();
						while(curPathIterator.hasNext())
							updatedPath.add(curPathIterator.next());
						updatedPath.add(adjNode);
						// Add emitted item to the mapping output
						Iterable<Tuple2<String,Integer>> emptyAdjList = 
								new ArrayList<Tuple2<String,Integer>>();
						Tuple2<String,Tuple3<Integer,Iterable<String>,Iterable<Tuple2<String,Integer>>>> newlyEmiitedPair =
								new Tuple2<String,Tuple3<Integer,Iterable<String>,Iterable<Tuple2<String,Integer>>>>(adjNode,
										new Tuple3<Integer,Iterable<String>,Iterable<Tuple2<String,Integer>>>(updatedDist,updatedPath,emptyAdjList));
						localEmittedPairs.add(newlyEmiitedPair);
					}
					return localEmittedPairs.iterator();
				});
		//DEBUG Print after emit
		System.out.println("After emittion:");
		emittedPairs.collect().forEach(System.out::println);
		
		// Action: Group by current node name
		
		JavaPairRDD<String,Iterable<Tuple3<Integer,Iterable<String>,Iterable<Tuple2<String,Integer>>>>> groupedEmitPairs = 
				emittedPairs.groupByKey();
		
		//DEBUG Print after grouping the emit pairs
		System.out.println("Group by cur node of emitted pairs:");
		groupedEmitPairs.collect().forEach(System.out::println);
	}

}
