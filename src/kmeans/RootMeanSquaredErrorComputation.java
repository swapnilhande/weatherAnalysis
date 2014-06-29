package kmeans;

import java.io.File;
import java.util.HashMap;
import java.util.List;

import org.apache.commons.io.FileUtils;

public class MeanSquaredErrorFinder {
	private final static String FINAL_OUTPUT_PATH = "C:/sem3/map_reduce/project/LocalKMeans1982/KmeanFinal1982V6/final_output.txt";
	private final static String CENTROID_VALUES_PATH = "C:/sem3/map_reduce/project/LocalKMeans1982/KmeanCluster1982V6/centroid_values.txt";
	private final static String AGGREGATOR_DATA = "C:/sem3/map_reduce/project/LocalKMeans1982/aggregator_data.txt";
	private final static int K_COUNTER = 6;
	public static double calculateMeanSquared(){
		HashMap<String, String> stationClusterMap = new HashMap<String, String>();
		HashMap<String, String> clusterCentroidValueMap = new HashMap<String,String>();
		double[] meanSquareErrorValues = new double[K_COUNTER] ;
		double totalSumOfClusters = 0;
		try {
				// Load Final Output of Cluster Key and Station Id into Station Cluster Map
				File finalOutput = new File(FINAL_OUTPUT_PATH);			
				List<String> outputLines = FileUtils.readLines(finalOutput);
				for(String line:outputLines){
					String[] data = line.replaceAll("\\t", " ").split(" ");
					stationClusterMap.put(data[1].trim(), data[0].trim());
				}
				
				// Load Centroid Values of cluster keys in Cluster Centroid Map	
				File centroidValues = new File(CENTROID_VALUES_PATH);
				List<String> centroidLines = FileUtils.readLines(centroidValues);
				for(String line:centroidLines){
					String[] data = line.replaceAll("\\t", " ").split(":");
					clusterCentroidValueMap.put(data[0].trim(), data[1].trim());
				}
				
				// Load and read the aggregator output file
				File aggregatorFile = new File(AGGREGATOR_DATA);
				List<String> aggLines = FileUtils.readLines(aggregatorFile);
				for(String line:aggLines){
					String[] data = line.replaceAll("\\t", " ").split(",");
					if(stationClusterMap.containsKey(data[0])){
						String clusterKey = stationClusterMap.get(data[0]);
						String clusterValue = clusterCentroidValueMap.get(clusterKey);
						String[] clusterMonthlyArray = clusterValue.split(" ");
						String[] aggLineArray = data[1].trim().split(" ");
						
						// Calculate the root mean squared error(rmse) of all points 
						// in the cluster with its cluster centroid
						int clusterKeyNumber = Integer.parseInt(clusterKey.substring(1)) - 1;
						double sumOfSquares = 0;
						for(int i=0; i<12; i++){
							double diff = Double.parseDouble(clusterMonthlyArray[i]) - Double.parseDouble(aggLineArray[i]);
							double squareOfDiff = Math.pow(diff, 2);
							sumOfSquares += squareOfDiff;
 						}
						meanSquareErrorValues[clusterKeyNumber] += Math.sqrt(sumOfSquares);
					 }
				}
				
				// calculate the sum of all rmse values across all clusters.
				for(int i=0 ;i<K_COUNTER;i++){
					double clusterMeanSquareError = meanSquareErrorValues[i]; 
					totalSumOfClusters += clusterMeanSquareError;
				}
		}
		catch(Exception ex){
			ex.printStackTrace();
		}
		
		return totalSumOfClusters;
	}

	public static void main(String[] args) {
		double value = calculateMeanSquared();
		System.out.println(value);
	}

}
