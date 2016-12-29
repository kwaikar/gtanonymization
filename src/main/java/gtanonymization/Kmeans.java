package gtanonymization;

import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.mllib.clustering.KMeans;
import org.apache.spark.mllib.clustering.KMeansModel;
import org.apache.spark.mllib.linalg.Vector;
import org.apache.spark.mllib.linalg.Vectors;
import org.apache.spark.sql.SQLContext;

import gtanonymization.domain.ColumnMetadata;
import gtanonymization.domain.DataMetadata;

public class Kmeans {
	public void trainModelAndPredict(DataMetadata dataMetadata, int numClusters) {

		SparkConf conf = new SparkConf().setAppName("JavaKMeansExample");
		conf.setMaster("local");
		JavaSparkContext jsc = new JavaSparkContext(conf);
		SQLContext sqlContext = new SQLContext(jsc);
		List<Vector> dataArray = extractRows(dataMetadata);
		JavaRDD<Vector> parsedData = jsc.parallelize(dataArray);
		parsedData.cache();
		// Cluster the data into two classes using KMeans
		int numIterations = 20;
		KMeansModel clusters = KMeans.train(parsedData.rdd(), numClusters, numIterations);
		System.out.println("Cluster centers:");
		for (Vector center : clusters.clusterCenters()) {
	//		System.out.println(" " + center);
		}
		System.out.println("Cluster center found for : ");
		System.out.println(dataArray.get(0));
System.out.println();
		int[] columnStartCounts = getColumnStartCounts(dataMetadata);
			Object[] returnObject = extractReturnObject(dataMetadata, columnStartCounts, dataArray.get(0).toArray());
			for (Object object : returnObject) {
				System.out.print(object+" ");
			}
			System.out.println();
			
 		System.out.println("as");
		System.out.println(clusters.clusterCenters()[clusters.predict(dataArray.get(0))]);
		System.out.println("-");
		for (Object object : extractReturnObject(dataMetadata, columnStartCounts, clusters.clusterCenters()[clusters.predict(dataArray.get(0))].toArray())) {
			System.out.print(object+" ");
		}
		System.out.println();
		System.out.println(
				"------------------------------------------------------------------------------------------------------------------------------------------------------");
		
		int[] numEntries = new int[clusters.clusterCenters().length];
		int index=0;
		for (Vector vc : dataArray) {
		numEntries[clusters.predict(vc)]++;	
		}
		int min=Integer.MAX_VALUE;
		 index=0;
		int minIndex=0;
		for (int i : numEntries) {
			
			if(i<min)
			{
				min=i;
				minIndex=index;
			}index++;
		}
		System.out.println("Smallest cluster found : "+minIndex+"-->"+min);
		for (Object object : extractReturnObject(dataMetadata, columnStartCounts, clusters.clusterCenters()[minIndex].toArray())) {
			System.out.print(object+" ");
		}
		
		//convertVectorToValue(dataMetadata, clusters.clusterCenters());
		System.out.println("Number of clusters found " + numClusters + "_" + clusters.clusterCenters().length);
		double cost = clusters.computeCost(parsedData.rdd());
		System.out.println("Cost: " + cost);

		// Evaluate clustering by computing Within Set Sum of Squared Errors
		double WSSSE = clusters.computeCost(parsedData.rdd());
		System.out.println("Within Set Sum of Squared Errors = " + WSSSE);

		jsc.stop();
	}

	public List<Object[]> convertVectorToValue(DataMetadata dataMetadata,Vector[] values) {
		List<Object[]> list = new LinkedList<Object[]>();

		int[] columnStartCounts = getColumnStartCounts(dataMetadata);
		for(Vector next:values) {
			
			double[] inputArrayVector = next.toArray();
			Object[] returnObject = extractReturnObject(dataMetadata, columnStartCounts, inputArrayVector);
			for (double value : inputArrayVector) {
				System.out.print(value+" ");
			}
			System.out.println();
			System.out.println("-->"+inputArrayVector.length+" | "+returnObject.length);
			System.out.println();
			for (Object object : returnObject) {
				System.out.print(object+" ");
			}
			System.out.println();
			list.add(returnObject);
		}
		
		return list;
	}

	private Object[] extractReturnObject(DataMetadata dataMetadata, int[] columnStartCounts,
			double[] inputArrayVector) {
		int index = 0;
		Object[] returnObject= new Object[dataMetadata.columns.length];
		for (ColumnMetadata<Comparable> column : dataMetadata.columns) {
			switch (
				column.getType()
			) {

			case 'i':
			case 'P':
				returnObject[index] = (int) inputArrayVector[columnStartCounts[index]];
				break;

			case 'd':
			case '$':
				returnObject[index] =  inputArrayVector[columnStartCounts[index]];
				break;
			/**
			 * add integer currency
			 */

				
			case 's':
				int position=columnStartCounts[index];
				double max=inputArrayVector[position];
				int maxPosition=position;
				for(int pos = position;pos<position+column.getNumUniqueValues();pos++)
				{
					if(max<inputArrayVector[pos])
					{
						max=inputArrayVector[pos];
						maxPosition=pos;
					}
				}
				returnObject[index] = column.getEntryAtPosition((maxPosition-columnStartCounts[index]));
				break;
			}
			index++;
}
		return returnObject;
	}



	public List<Vector> extractRows(DataMetadata dataMetadata) {
		List<Vector> list = new LinkedList<Vector>();
		int[] columnStartCounts = getColumnStartCounts(dataMetadata);
		for (Object[] ds : dataMetadata.rows) {
			double[] row = new double[getTotalNewColumns(dataMetadata)];
			int index = 0;
			for (ColumnMetadata<Comparable> column : dataMetadata.columns) {
				switch (
					column.getType()
				) {
				/**
				 * Add integer value
				 */
				case 'i':
				case 'P':
					row[columnStartCounts[index]] = ((Integer) ds[index]);
					break;

				/**
				 * Add double value
				 */
				case 'd':
				case '$':
					row[columnStartCounts[index]] = ((Double) ds[index]);
					break;
				/**
				 * add integer currency
				 */

				case 's':
					row[(columnStartCounts[index] + column.getIndex((String) ds[index]))] = 1.0;
					break;
				}
				index++;
			}

			list.add(Vectors.dense(row));
		}
		return list;
	}

	/**
	 * This function returns total number of columns applicable in the vector, it also takes 
	 * number of unique values available in string column into account..
	 * @param dataMetadata
	 * @return
	 */
	public int getTotalNewColumns(DataMetadata dataMetadata) {
		int startCount = 0;
		for (ColumnMetadata<Comparable> column : dataMetadata.columns) {
			switch (
				column.getType()
			) {
			case 's':
				startCount += column.getNumUniqueValues();
				break;
			default:
				startCount += 1;
				break;
			}
		}
		return startCount;
	}
	
	/**
	 * This function returns beginning of each of the column in Kmeans Vector.
	 * @param dataMetadata
	 * @return
	 */
	private int[] getColumnStartCounts(DataMetadata dataMetadata) {
		int startCount = 0;
		int[] columnStartCounts = new int[dataMetadata.columns.length];
		int index = 0;
		for (ColumnMetadata<Comparable> column : dataMetadata.columns) {
			switch (
				column.getType()
			) {
			case 's':
				columnStartCounts[index++] = startCount;
				startCount += column.getNumUniqueValues();

				break;
			default:
				columnStartCounts[index++] = startCount;
				startCount += 1;
				break;
			}
		}
		return columnStartCounts;
	}

}