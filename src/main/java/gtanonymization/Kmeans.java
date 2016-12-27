package gtanonymization;

import java.util.LinkedList;
import java.util.List;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.mllib.clustering.KMeans;
import org.apache.spark.mllib.clustering.KMeansModel;
import org.apache.spark.mllib.linalg.Vector;
import org.apache.spark.mllib.linalg.Vectors;
import org.apache.spark.sql.SQLContext;

import gtanonymization.domain.ColumnMetadata;
import gtanonymization.domain.DataMetadata;

public class Kmeans {
	public void trainModelAndPredict (DataMetadata dataMetadata){

		
		SparkConf conf = new SparkConf().setAppName("JavaKMeansExample");
		JavaSparkContext jsc = new JavaSparkContext(conf);
		SQLContext sqlContext = new SQLContext(jsc);

		JavaRDD<double[]> data =  jsc.parallelize(extractRows(dataMetadata));
		
		JavaRDD<Vector> parsedData = data.map(new Function<double[], Vector>() {
			private static final long serialVersionUID = 1L;
			public Vector call(double[] values) {
				return Vectors.dense(values);
			}
		});
		parsedData.cache();

		// Cluster the data into two classes using KMeans
		int numClusters = 2;
		int numIterations = 20;
		KMeansModel clusters = KMeans.train(parsedData.rdd(), numClusters, numIterations);

		System.out.println("Cluster centers:");
		for (Vector center : clusters.clusterCenters()) {
			System.out.println(" " + center);
		}
		double cost = clusters.computeCost(parsedData.rdd());
		System.out.println("Cost: " + cost);

		// Evaluate clustering by computing Within Set Sum of Squared Errors
		double WSSSE = clusters.computeCost(parsedData.rdd());
		System.out.println("Within Set Sum of Squared Errors = " + WSSSE);

		/*clusters.save(jsc.sc(), "KMeansModel");
		KMeansModel sameModel = KMeansModel.load(jsc.sc(), "KMeansModel");*/
		jsc.stop();
	}
	
	

	public List<double[]> extractRows( DataMetadata dataMetadata) {
		List<double[]> list = new LinkedList<double[]>();
		int startCount = 0;
		int[] columnStartCounts=new int[dataMetadata.columns.length];
		int index=0;
		for (ColumnMetadata<Comparable> column : dataMetadata.columns) {
			switch (
				column.getType()
				) {
			case 's':
				columnStartCounts[index++]=startCount;
				startCount += column.getNumUniqueValues();

				break;
			default:
				columnStartCounts[index++]=startCount;
				startCount += 1;
				break;
			}
		}
		for (Object[] ds : dataMetadata.rows) {
			double[] row = new double[startCount]; 
			  index = 0;
			for (ColumnMetadata<Comparable> column : dataMetadata.columns) {
				switch (
						column.getType()
					) {
					/**
					 * Add integer value
					 */
					case 'i':
					case 'P':
						row[index] =  ((Integer)ds[index]);
						break;

					/**
					 * Add double value
					 */
					case 'd':
					case '$':
						row[index] =  ((Double)ds[index]);
						break;
					/**
					 * add integer currency
					 */
 
					case 's':
 						row[(columnStartCounts[index]+column.getIndex((String)ds[index]))] =1.0;
						break;
					}
				index++;
			}
			list.add(row);
		}
		return list;
	}

}