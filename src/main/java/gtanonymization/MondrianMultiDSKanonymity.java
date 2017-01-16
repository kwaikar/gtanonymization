/**
 * 
 */
package gtanonymization;

import java.util.Arrays;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Set;

import org.apache.commons.lang3.tuple.ImmutablePair;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.log4j.Logger;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.serializer.KryoRegistrator;

import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.serializers.FieldSerializer;

import gtanonymization.domain.ColumnStatistics;
import gtanonymization.domain.Row;
import scala.Tuple2;

/**
 * This class implements mondrian multi-dimensional k-anonymity.
 * 
 * @author kanchan
 */
public class MondrianMultiDSKanonymity {
	final static Logger logger = Logger.getLogger(MondrianMultiDSKanonymity.class);

	private ColumnStatistics[] columns = null;
	private JavaSparkContext jsc = null;
	static boolean[] isQuantitative = null;
	List<List<Row>> equivalentClasses = new LinkedList<List<Row>>();

	/**
	 * This method accepts rows, Column Heuristics and
	 * 
	 * @param rows
	 * @param k
	 * @return
	 */
	public MondrianMultiDSKanonymity(ColumnStatistics[] columns) {

		SparkConf conf = new SparkConf().setAppName("JavaKMeansExample");

		conf.set("spark.serializer", "org.apache.spark.serializer.KryoSerializer");
		conf.set("spark.kryo.registrator", RowRegistrator.class.getName());
		Class[] classes = new Class[1];
		classes[0] = Row.class;
		conf.registerKryoClasses(classes);

		conf.setMaster("local");
		jsc = new JavaSparkContext(conf);
		this.columns = columns;
		isQuantitative = getQuantitativeIndices();
		jsc.broadcast(isQuantitative);
	}

	/**
	 * @return the equivalentClasses
	 */
	public List<List<Row>> getEquivalentClasses() {
		return equivalentClasses;
	}

	public void destroy() {
		jsc.stop();
	}

	/**
	 * This method performs multi dimensional cuts. It uses range heuristics in
	 * order to select the column to perform cut on. Works only on quantitative
	 * columns
	 * 
	 * @param rows
	 * @param k
	 */

	public void anonymize(List<Row> rows, int k, boolean[] columnsEnabledForCut) {
		if (rows.size() < k) {
			System.out.println("No cut allowed. Number of rows present");
		}
		else if (rows.size() == k) {
			System.out.println("Cluster cannot be paritioned further.");
			equivalentClasses.add(rows);
		}
		else {

			JavaRDD<Row> rowsRDD = jsc.parallelize(rows);
			final int dim = selectDimension(rowsRDD, columnsEnabledForCut);

			if (dim >= 0) {
				columnsEnabledForCut[dim] = false;
				boolean[] columnsEnabledForCut1 = Arrays.copyOf(columnsEnabledForCut, columnsEnabledForCut.length);
				boolean[] columnsEnabledForCut2 = Arrays.copyOf(columnsEnabledForCut, columnsEnabledForCut.length);
				
				sortRowsByDimensionChosen(rows, dim);
 
				Object min = rows.get(0).row[dim];
				Object max = rows.get(rows.size() - 1).row[dim];
				Object median = rows.get(rows.size() / 2).row[dim];
				
				List<Row>  leftSet = rows.subList(0, (rows.size() / 2));
				List<Row>  rightSet = rows.subList((rows.size() / 2), rows.size());

				
				logger.info(
						"Cut performed on " + columns[dim].getColumnName() + ": " + min + " : " + median + " : " + max);
				logger.info("Dividing into two" + leftSet + " : " + rightSet);

				if (leftSet.size() >= k && rightSet.size() >= k) {

					Pair<Object,Object> leftPair =new ImmutablePair<Object, Object>(min,median);
					
					for (Row row : leftSet) {
						row.setNewRow(leftPair, dim);
					}
					Pair<Object,Object> rightPair =new ImmutablePair<Object, Object>(median, max);
					for (Row row : rightSet) {
						row.setNewRow(rightPair, dim);
					}
					anonymize(leftSet, k, columnsEnabledForCut1);
					anonymize(rightSet, k, columnsEnabledForCut2);
				}
				else {
					equivalentClasses.add(rows);
				}
			}
			else {
				equivalentClasses.add(rows);
			}
		}
	}

	private void sortRowsByDimensionChosen(List<Row> rows, final int dim) {
		Collections.sort(rows, new Comparator<Row>() {
			public int compare(Row o1, Row o2) {
				switch (
					columns[dim].getType()
				) {
				case 'i':
				case 'P':
					return ((Integer) o1.getRow(dim)).compareTo((Integer) (o2.getRow(dim)));
				case 'd':
				case '$':
					return ((Double) o1.getRow(dim)).compareTo((Double) (o2.getRow(dim)));
				default:
					return -1;
				}
			}

		});
	}

	public static class RowRegistrator implements KryoRegistrator {
		public void registerClasses(Kryo kryo) {
			kryo.register(Row.class, new FieldSerializer(kryo, Row.class));
		}
	}

	/**
	 * The dimension needs to be selected based on the column which has maximum
	 * range, i.e. unique values.
	 * 
	 * @param rows
	 * @return
	 */

	public int selectDimension(JavaRDD<Row> rowsRDD, boolean[] isQuantitative) {

		/**
		 * Emit <Index,value> - since multiple values need to be emitted,
		 * flatmap is used.
		 */

		JavaRDD<Tuple2<Integer, Object>> map = rowsRDD.flatMap(rowToIndexValueFunction);
		/**
		 * convert into key,value pair RDD.
		 */
		// System.out.println("1 ===>" + map.collect());

		JavaPairRDD<Integer, Iterable<Object>> mapPairs = map.mapToPair(tupleToPairFunction).groupByKey().cache();

		// System.out.println("2 ===>" + mapPairs.collect().toString());
		/**
		 * Calculate numCounts for each index.
		 */
		JavaPairRDD<Integer, Integer> numUniqueEntries = mapPairs.mapValues(indexToUniqueCountsFunction);
		// System.out.println("3====>" + numUniqueEntries.collect());
		/**
		 * Reverse the map in order to find the max numUnique Value, sort by
		 * key.
		 */

		JavaPairRDD<Integer, Integer> reverseMap = numUniqueEntries.mapToPair(reverseTuplePairFunction)
				.sortByKey(false);

		// System.out.println("4 -----------------------------" +
		// reverseMap.collect());

		List<Tuple2<Integer, Integer>> frequencyColumnTuples = reverseMap.collect();
		for (Tuple2<Integer, Integer> tuple2 : frequencyColumnTuples) {
			if (isQuantitative[tuple2._2]) {
				// System.out.println("5--------------" + tuple2._2);
				return tuple2._2;
			}

		}
		System.out.println("Map is found to be empty");
		return -1;
	}

	static FlatMapFunction rowToIndexValueFunction = new FlatMapFunction<Row, Tuple2<Integer, Object>>() {

		public Iterator<Tuple2<Integer, Object>> call(Row row) throws Exception {
			Set<Tuple2<Integer, Object>> set = new HashSet<Tuple2<Integer, Object>>();
			int count = 0;
			for (Object column : row.row) {
				if (isQuantitative[count]) {
					set.add(new Tuple2(count, column));
				}
				count++;
			}
			return set.iterator();
		};
	};
	static PairFunction tupleToPairFunction = new PairFunction<Tuple2<Integer, Object>, Integer, Object>() {
		public Tuple2<Integer, Object> call(Tuple2<Integer, Object> tuple) {
			return tuple;
		};
	};
	static Function indexToUniqueCountsFunction = new Function<Iterable<Object>, Integer>() {
		public Integer call(Iterable<Object> rs) {
			HashSet<Object> set = new HashSet<Object>();
			for (Object result : rs) {
				set.add(result);
			}
			return set.size();
		}
	};
	static PairFunction reverseTuplePairFunction = new PairFunction<Tuple2<Integer, Integer>, Integer, Integer>() {
		public Tuple2<Integer, Integer> call(Tuple2<Integer, Integer> tuple) {
			return new Tuple2<Integer, Integer>(tuple._2, tuple._1);
		};
	};

	private boolean[] getQuantitativeIndices() {
		final boolean[] isQuantitative = new boolean[columns.length];
		int index = 0;
		for (ColumnStatistics<Comparable> columnStatistics : columns) {
			switch (
				columnStatistics.getType()
			) {
			case 'i':
			case 'P':
			case 'd':
			case '$':
				if (columnStatistics.isQuasiIdentifier()) {
					isQuantitative[index] = true;
				}
				else {
					isQuantitative[index] = false;
				}
				break;
			default:
				isQuantitative[index] = false;
				break;
			}
			index++;
		}
		return isQuantitative;
	}

}
