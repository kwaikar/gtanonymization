package gtanonymization;

import java.util.LinkedList;
import java.util.List;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.ml.feature.BucketedRandomProjectionLSH;
import org.apache.spark.ml.feature.BucketedRandomProjectionLSHModel;
import org.apache.spark.ml.linalg.DenseVector;
import org.apache.spark.ml.linalg.Vector;
import org.apache.spark.ml.linalg.VectorUDT;
import org.apache.spark.ml.linalg.Vectors;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.SQLContext;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.Metadata;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;

import gtanonymization.domain.ColumnMetadata;
import gtanonymization.domain.DataMetadata;

/**
 * This class implements spark-LSH algorithm on Adults uci dataset.
 * 
 * @author kanchan
 */
public class LSH {

	public void trainModelAndPredict(DataMetadata dataMetadata, int clusterSize) {

		SparkConf conf = new SparkConf().setAppName("JavaKMeansExample");
		conf.setMaster("local");
		JavaSparkContext jsc = new JavaSparkContext(conf);
		List<Row> dataArray = extractRows(dataMetadata);

		SQLContext sqlContext = new SQLContext(jsc);

		StructType schema = new StructType(
				new StructField[] { new StructField("id", DataTypes.IntegerType, false, Metadata.empty()),
						new StructField("keys", new VectorUDT(), false, Metadata.empty()) });

		Dataset<Row> dfA = sqlContext.createDataFrame(dataArray, schema);

		BucketedRandomProjectionLSH mh = new BucketedRandomProjectionLSH().setBucketLength(5.0).setNumHashTables(40)
				.setInputCol("keys").setOutputCol("values");

		BucketedRandomProjectionLSHModel model = mh.fit(dfA);

		// Feature Transformation
		Dataset<Row> transformedModel = model.transform(dfA);
		transformedModel.show();

		List<Row> neighbors = (List<Row>) model.approxNearestNeighbors(transformedModel,
				(Vector) Vectors
						.dense(extractRow(dataMetadata, getColumnStartCounts(dataMetadata), dataMetadata.rows.get(0))),
				clusterSize).collectAsList();
		System.out.println();
		int[] columnStartCounts = getColumnStartCounts(dataMetadata);
		System.out.println("Neighbors for following entry are:");
		printObject(dataMetadata, columnStartCounts, ((DenseVector)dataArray.get(0).get(1)).toArray());
		System.out.println("As follows: ");
		for(Row neighbor :neighbors)
		{
			printObject(dataMetadata, columnStartCounts, ((DenseVector)neighbor.get(1)).toArray());
		}

		jsc.stop();
	}

	public void printObject(DataMetadata dataMetadata, int[] columnStartCounts,double[] vector)
	{
		Object[] returnObject = extractReturnObject(dataMetadata, columnStartCounts, vector);
		for (Object object : returnObject) {
			System.out.print(object + " ");
		}
		System.out.println();
		
	}
	public List<Object[]> convertVectorToValue(DataMetadata dataMetadata, Row[] values) {
		List<Object[]> list = new LinkedList<Object[]>();

		int[] columnStartCounts = getColumnStartCounts(dataMetadata);
		for (Row next : values) {

			Row inputArrayVector = next;
			Object[] returnObject = extractReturnObject(dataMetadata, columnStartCounts,
					((DenseVector) inputArrayVector.get(0)).toArray());
			for (int i = 0; i < inputArrayVector.size(); i++) {
				System.out.print(inputArrayVector.getDouble(i) + " ");
			}
			System.out.println();
			System.out.println("-->" + inputArrayVector.length() + " | " + returnObject.length);
			System.out.println();
			for (Object object : returnObject) {
				System.out.print(object + " ");
			}
			System.out.println();
			list.add(returnObject);
		}

		return list;
	}

	private Object[] extractReturnObject(DataMetadata dataMetadata, int[] columnStartCounts,
			double[] inputArrayVector) {
		int index = 0;
		Object[] returnObject = new Object[dataMetadata.columns.length];
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
				returnObject[index] = inputArrayVector[columnStartCounts[index]];
				break;
			/**
			 * add integer currency
			 */

			case 's':
				int position = columnStartCounts[index];
				double max = inputArrayVector[position];
				int maxPosition = position;
				for (int pos = position; pos < position + column.getNumUniqueValues(); pos++) {
					if (max < inputArrayVector[pos]) {
						max = inputArrayVector[pos];
						maxPosition = pos;
					}
				}
				returnObject[index] = column.getEntryAtPosition((maxPosition - columnStartCounts[index]));
				break;
			}
			index++;
		}
		return returnObject;
	}

	public List<Row> extractRows(DataMetadata dataMetadata) {
		List<Row> list = new LinkedList<Row>();
		int[] columnStartCounts = getColumnStartCounts(dataMetadata);
		int rowCount = 0;
		for (Object[] ds : dataMetadata.rows) {
			double[] row = extractRow(dataMetadata, columnStartCounts, ds);
			list.add(RowFactory.create(rowCount++, Vectors.dense(row)));
		}
		return list;
	}

	private double[] extractRow(DataMetadata dataMetadata, int[] columnStartCounts, Object[] ds) {
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
				row[(columnStartCounts[index] + column.getIndex((String) ds[index]))] = 100.0;
				break;
			}
			index++;
		}
		return row;
	}

	/**
	 * This function returns total number of columns applicable in the vector,
	 * it also takes
	 * number of unique values available in string column into account..
	 * 
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
	 * 
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