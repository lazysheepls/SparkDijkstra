import java.io.File;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;

import org.apache.commons.io.FileUtils;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

import scala.Tuple2;
import scala.Tuple3;
/**
 * Executive Summary
 * 1. Read from input file
 * 2. Process input content and map to <starting node, <end node, distance>> format (MapToPair)
 * 3. Get total number of nodes (used to determine number of iteration needed)
 * 4. Group by key to create adjacent list for each node (GroupByKey)
 * 5. Restructure input format to add path list and distance (MapToPair)
 * 6. Iterate to find shortest distance and update path if exist. This will iterate (NumOfNodes - 1) times
 *    to make sure all nodes excluding the start node are examined
 * 7. Sort result in ascending order and write to output file
 * */
public class AssigTwoz3451444 {

	public static void main(String[] args) throws Exception{
		SparkConf conf = new SparkConf().setMaster("local").setAppName("Ass2");
		
		// Process input arguments
		String init_start_node = args[0];
		String input_path = args[1];
		String output_path = args[2];
		
		File output_folder_to_delete = new File("output");
		if(output_folder_to_delete.exists()) {
			FileUtils.cleanDirectory(output_folder_to_delete);
			FileUtils.deleteDirectory(output_folder_to_delete);
		}
		
		/**
		 * Process input file
		 * */
		JavaSparkContext context = new JavaSparkContext(conf);
		JavaRDD<String> inputRDD = context.textFile(input_path);
		
		/**
		 * Transformation: input RDD to pairs
		 * 1. Split input string by delimiter ","
		 * 2. Map to pair with format <starting node, <end node, distance>>
		 * */
		JavaPairRDD<String,Tuple2<String,Integer>> inputPairs = inputRDD.mapToPair(line -> {
			String[] items = line.split(",");
			Tuple2<String, Integer> destAndDistPair = new Tuple2<String, Integer>(items[1], Integer.parseInt(items[2]));
			return new Tuple2<String,Tuple2<String,Integer>>(items[0],destAndDistPair);
		});
		
		/**
		 * Get total number of nodes in the graph
		 * Purpose: this number is later used as the number of iteration
		 * 1. Read each line of the input, add both "starting node" and "destination node" to the allNodeNamesRDD (flatMap)
		 * 2. Remove duplicated names and save distinct node names into nodeNameList
		 * 3. Number of nodes is the count of nodeNamesListRDD
		 * */
		JavaRDD<String> allNodeNamesRDD = inputPairs.flatMap(item ->{
			String curNode = item._1;
			String destNode = item._2._1;
			List<String> nodeNamesList = new ArrayList<String>();
			nodeNamesList.add(curNode);
			nodeNamesList.add(destNode);
			return nodeNamesList.iterator();
		});
		
		JavaRDD<String> distinctNodeNamesRDD = allNodeNamesRDD.distinct();
		int numberOfNodes = (int) distinctNodeNamesRDD.count();
		
		/**
		 * Action: Group by key to create adjacent list
		 * Key is the "Starting node" from the inputPairs JavaPairRDD
		 * */
		JavaPairRDD<String,Iterable<Tuple2<String,Integer>>> inputWithAdjList = inputPairs.groupByKey();
		
		/**
		 * Transformation: Restructure input, add initial path and distance information
		 * Expected output format <current node, <distance, path list, adjacent list>
		 * To initialize the new structure: 
		 * - If node is not the initial start node -> set distance to infinite (max integer)
		 * - If node is the initial start node -> set distance to 0
		 * - Add current node to the path list to initialize the path list
		 * */
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
		
		/**
		 * Iteration: Find shortest path
		 * After each iteration, the shortest path will be updated. 
		 * Here, the number of iteration is the same as the number of nodes in the graph -1 (excluding the start node) 
		 * to make sure each node has been scanned through.
		 * (Details of the IterateOnceToUpdateShortestRoute function is explained at the function declaration)
		 * */
		JavaPairRDD<String,Tuple3<Integer,Iterable<String>,Iterable<Tuple2<String,Integer>>>> updatedRoutes = inputWithPathAndAdjList;
		for (int i=0;i< numberOfNodes-1;i++) {
			updatedRoutes = IterateOnceToUpdateShortestRoute(updatedRoutes);
		}
		
		/**
		 * Save result to the output file
		 * (Details of the PrintShorestPath function is explained at the function declaration)
		 * */
		PrintShortestPath(updatedRoutes,init_start_node,output_path);
	}
	
	/**
	 * Function for each iteration: IterateOnceToUpdateShortestRoute
	 * Input: JavaPairRDD<current node,<distance, path list, adjacent list>>
	 * Output: JavaPairRDD<current node,<distance, path list, adjacent list>>
	 * 1. Emit from one to more routes: From adjacent list, expand the routes from start node to the nodes in the adjacent list
	 * 2. Group by current node: Get a list of possible routes from start node to current node
	 * 3. Find shortest distance and update path if exist: Find shortest distance in all possible routes
	 * */
	public static JavaPairRDD<String,Tuple3<Integer,Iterable<String>,Iterable<Tuple2<String,Integer>>>>
	IterateOnceToUpdateShortestRoute (JavaPairRDD<String,Tuple3<Integer,Iterable<String>,Iterable<Tuple2<String,Integer>>>> 
	inputWithPathAndAdjList) {
		/**
		 * 1. Emit from one to more routes (FlatMapToPair)
		 * */
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
		
		/**
		 * 2. Group by current node: 
		 * Get a list of possible routes from start node to current node
		 * */
		JavaPairRDD<String,Iterable<Tuple3<Integer,Iterable<String>,Iterable<Tuple2<String,Integer>>>>> groupedEmitPairs = 
				emittedPairs.groupByKey();
		
		/**
		 * 3. Find shortest distance and update path if exist
		 * */
		JavaPairRDD<String,Tuple3<Integer,Iterable<String>,Iterable<Tuple2<String,Integer>>>> EmitPairsAfterIter = 
				groupedEmitPairs.mapToPair(item -> {
					// Initialize
					String curNode = item._1;
					List<Tuple2<String,Integer>> newAdjList = new ArrayList<Tuple2<String,Integer>>();
					List<String> newPathList = new ArrayList<String>();
					
					// Set initial path and copy adjacent list (Same across all possible routes)
					Iterator<Tuple3<Integer,Iterable<String>,Iterable<Tuple2<String,Integer>>>> routesIterator_1 = item._2().iterator();
					
					while(routesIterator_1.hasNext()) {
						Tuple3<Integer,Iterable<String>,Iterable<Tuple2<String,Integer>>> curRoute = routesIterator_1.next();
						Iterator<Tuple2<String,Integer>> adjIterator = curRoute._3().iterator();
						Iterator<String> pathIterator = curRoute._2().iterator();
						if(adjIterator.hasNext()) {
							while (adjIterator.hasNext()) {
								newAdjList.add(adjIterator.next());
							}
							while (pathIterator.hasNext()) {
								newPathList.add(pathIterator.next());
							}
							break;
						}	
					}
					
					// Initialize before iterating through all possible routes to find the shortest route
					Iterator<Tuple3<Integer,Iterable<String>,Iterable<Tuple2<String,Integer>>>> routesIterator_2 = item._2().iterator();
					int shortestDist = Integer.MAX_VALUE;
					
					
					while(routesIterator_2.hasNext()) {
						Tuple3<Integer,Iterable<String>,Iterable<Tuple2<String,Integer>>> curRoute = routesIterator_2.next();
						int curDist = curRoute._1();
						Iterator<String> curPathIterator = curRoute._2().iterator();
						if (curDist < shortestDist) {
							// Update shortest distance
							shortestDist = curDist;
							// Update path
							newPathList = new ArrayList<String>();
							while(curPathIterator.hasNext())
								newPathList.add(curPathIterator.next());
						}
					}
					
					return new Tuple2<String,Tuple3<Integer,Iterable<String>,Iterable<Tuple2<String,Integer>>>>(
							curNode, new Tuple3<Integer,Iterable<String>,Iterable<Tuple2<String,Integer>>>(
									shortestDist,newPathList,newAdjList));
				});
		
		return EmitPairsAfterIter;
	}
	
	/**
	 * Function: Save result to the output file
	 * 1. Remove initial start node from the result
	 * 2. Restructure the result: Set distance as key (MapToPair)
	 * 3. Sort result in ascending order by distance (GroupByKey)
	 * 4. Map sorted result into a list of formatted string (Map)
	 * 5. Save to user defined output path
	 * */
	public static void PrintShortestPath(JavaPairRDD<String,Tuple3<Integer,Iterable<String>,Iterable<Tuple2<String,Integer>>>> 
	inputWithPathAndAdjList, String startNode, String filePath) throws Exception {
		/**
		 * 1. Remove initial start node from result
		 * */
		JavaPairRDD<String,Tuple3<Integer,Iterable<String>,Iterable<Tuple2<String,Integer>>>> filteredResult = 
				inputWithPathAndAdjList.filter(item -> (!item._1.equals(startNode)));
		
		/**
		 * 2. Restructure: Set distance as key
		 * */
		JavaPairRDD<Integer,Tuple2<String,Iterable<String>>> restructedResult = filteredResult.mapToPair(item -> {
			String curNode = item._1();
			int distance = item._2()._1();
			Iterable<String> path = item._2()._2();
			Tuple2<String,Iterable<String>> curNodeAndPath = new Tuple2<String,Iterable<String>>(curNode,path);
			return new Tuple2<Integer,Tuple2<String,Iterable<String>>>(distance, curNodeAndPath);
		});
		
		/**
		 * 3. Sort result by distance in ascending order
		 * */
		JavaPairRDD<Integer,Tuple2<String,Iterable<String>>> sortedResult = restructedResult.sortByKey(true);
		
		/**
		 * 4. Format the string and map to resultRDD
		 * */
		JavaRDD<String> resultRDD = sortedResult.map(item -> {
			String curNode = item._2._1();
			int distance = item._1;
			Iterator<String> pathIterator = item._2._2().iterator();
			String outputLine = "";
			
			outputLine += curNode;
			outputLine +=",";
			if (distance == Integer.MAX_VALUE) {
				outputLine += "-1";
			}	
			else {
				outputLine += Integer.toString(distance);
				outputLine += ",";
				if(pathIterator.hasNext()) //first node in path
					outputLine += pathIterator.next();
				while(pathIterator.hasNext()) {
					outputLine += "-";
					outputLine += pathIterator.next();
				}
			}
			return outputLine;
		});
		
		/**
		 * 5. Write to file
		 * */
		resultRDD.saveAsTextFile(filePath);
	}
}
