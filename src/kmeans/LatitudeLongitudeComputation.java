package kmeans;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileReader;
import java.io.FileWriter;
import java.util.HashMap;
import java.util.List;

import org.apache.commons.io.FileUtils;

public class LatitudeComputation {
	public static void mergeData(){
 		HashMap<String, String> clusterData = new HashMap<String,String>();
		try {
			File latitudeFile = new File("C:/sem3/map_reduce/project/LatLongData/ish-history.txt");
			File folder = new File("C:/sem3/map_reduce/project/LatLongData/input/");
			File[] listOfFiles = folder.listFiles();
			
			// Load all the final Cluster key and Station Id data in HashMap
			for (int i = 0; i < listOfFiles.length; i++) {
			  File file = listOfFiles[i];
			  if (file.isFile() && !file.getName().toLowerCase().contains("_success")) {
			    List<String> listOfLines = FileUtils.readLines(file);
			    for(String line:listOfLines){
				    String[] data = line.replaceAll("\\t", " ").split(" ");
					clusterData.put(data[1].trim(), data[0].trim());
			    }
			    
			    System.out.println(clusterData);
			    System.out.println(clusterData.size());
			  } 
			}
			
			// Create Output File
			String latLongLine;
			File file = new File("C:/sem3/map_reduce/project/LatLongData/2012visualk5.txt");
			if(!file.exists()){
				file.createNewFile();
			}
			
			FileWriter fileWriter = new FileWriter(file.getAbsoluteFile(),true);
			BufferedWriter bufferedWriter = new BufferedWriter(fileWriter);
			BufferedReader readLatLongFile = new BufferedReader(new FileReader(latitudeFile));
			StringBuffer sb = new StringBuffer();
			sb.append("Cluster,StationId,Latitude,Longitude");
			sb.append("\n");
			// Calculate latitude and longitude values based on station Ids 
			// and club together with Cluster Key
			int counter = 0;
			int count = 0;
			while((latLongLine = readLatLongFile.readLine())!= null){
				count++;
				String[] latLongArray = latLongLine.trim().split(",");
				String stationId = latLongArray[0].toString().substring(1,latLongArray[0].length()-1);
				boolean containsKey = clusterData.containsKey(stationId);
				if(containsKey){
					counter++;
					String latitude = latLongArray[7].substring(1,latLongArray[7].length()-1);
					if(!latitude.isEmpty()){
						String Finallatitude = Float.toString(Float.parseFloat(latitude) / 1000);
						
						String longitude = latLongArray[8].substring(1,latLongArray[8].length()-1);
						String Finallongitude = Float.toString(Float.parseFloat(longitude) / 1000);
						
						sb.append(clusterData.get(stationId) + "," + latLongArray[0] + ","+
								Finallatitude + "," + Finallongitude);
						sb.append("\n");
						
					}
				}
			}
			bufferedWriter.write(sb.toString());
			bufferedWriter.close();
			System.out.println("total count: " + count);
			System.out.println("number of times inside: " + counter);
			System.out.println("Done!");
			readLatLongFile.close();
		}
		catch (Exception e) {
			System.out.println("File not found");
			e.printStackTrace();
		}
	}
	public static void main(String[] args) {
		LatitudeComputation.mergeData();
	}

}
