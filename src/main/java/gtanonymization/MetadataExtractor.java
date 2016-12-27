package gtanonymization;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Set;

import org.apache.commons.io.FileUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.log4j.Logger;

import gtanonymization.domain.ColumnMetadata;
import gtanonymization.domain.DataMetadata;
import scala.collection.parallel.ParIterableLike.Foreach;

/**
 * This class extracts metadata for each column from the input. It takes input
 * file,
 * for integer columns, extracts min, max and keeps counts for each column in
 * order to calculate probability for the same.
 * 
 * @author kanchan
 */
public class MetadataExtractor {

	final static Logger logger = Logger.getLogger(MetadataExtractor.class);

	/**
	 * This method extracts basic statistics about the data.
	 * 
	 * @param data
	 *            - data from which statistics need to be extracted
	 * @param headerLine
	 *            - optional. If present, it considers first line of "data" as
	 *            data.
	 * @throws InvalidInputFileException
	 */
	public DataMetadata extractStatistics(String data, String headerLine) throws InvalidInputFileException {
		int numColumns = 0;
		int dataStartCount = 0;
		String[] lines = data.split("\n");
		/**
		 * Extract number of columns
		 */
		if (lines.length > 2) {
			numColumns = (lines[0].split(Constants.COMMA).length == lines[1].split(Constants.COMMA).length)
					? lines[0].split(Constants.COMMA).length : 0;
		}

		if (StringUtils.isBlank(headerLine)) {
			headerLine = lines[0];
			dataStartCount = 1;
		}
		if (numColumns <= 0) {
			throw new InvalidInputFileException("Zero columns found in the dataset.");
		}

		DataMetadata dataMetadata = basicMetadata(headerLine, numColumns, dataStartCount, lines);
		logger.info(dataMetadata); 
		Kmeans kmeans = new Kmeans();
		kmeans.trainModelAndPredict(dataMetadata);
		// extractClusters(dataStartCount, lines, dataMetadata);
		// LatticeCreator lc = new LatticeCreator();
		// lc.formTree(lines[dataStartCount].split(","), dataMetadata);
		logger.info("done!");
		return dataMetadata;
	}

	/**
	 * This is a 2 pass algorithm in which in first pass, we find out all
	 * entries that are within the cluster, in second round, we find the point
	 * for first one to hang to.
	 * 
	 * @param dataStartCount
	 * @param lines
	 * @param dataMetadata
	 */
	private void extractClusters(int dataStartCount, String[] lines, DataMetadata dataMetadata) {
		List<Set<Integer>> clusters = new LinkedList<Set<Integer>>();
		boolean[] isDisabled = new boolean[lines.length - dataStartCount];
		List<Integer> outliers = new ArrayList<Integer>();
		/**
		 * First Pass- assign obvious cluster
		 */

		for (int i = dataStartCount; i < lines.length; i++) {
			if (!isDisabled[i]) {

				Set<Integer> cluster = new HashSet<Integer>();
				int minDistance = Integer.MAX_VALUE;
				int minFound = -1;
				logger.info(lines[i]);
				for (int j = dataStartCount; j < lines.length; j++) {
					if (i != j && !isDisabled[j]) {
						int distanceFound = DistanceExtractor.getDistance(dataMetadata, lines[i], lines[j]);
						if (distanceFound < Constants.CLUSTER_WIDTH) {
							minDistance = distanceFound;
							isDisabled[j] = true;
							cluster.add(j);
							minFound = j;
							logger.info(lines[j]);
						}
						else if (minDistance > distanceFound) {
							minDistance = distanceFound;
							minFound = j;
						}
					}
				}
				isDisabled[i] = true;
				cluster.add(i);
				if (minDistance > Constants.CLUSTER_THRESHOLD) {
					outliers.add(i);
				}
				else {
					isDisabled[minFound] = true;
					cluster.add(minFound);
					clusters.add(cluster);
					logger.info("Cluster found : " + cluster.size());
				}
				// logger.info(lines[minFound]);
				// logger.info(i + ":(" + minFound + "=>" + minDistance);
			}
		}
		logger.info("Clusters found:" + clusters.size());
		logger.info("Number of Outliers found " + outliers);
		/**
		 * Pass 2
		 */
		logger.info("-------------------------Executing Pass 2----------------------------");
		for (Integer i : outliers) {

			Set<Integer> cluster = new HashSet<Integer>();
			int minDistance = Integer.MAX_VALUE;
			int minFound = -1;
			for (int j = dataStartCount; j < lines.length; j++) {
				if (i != j) {

					int distanceFound = DistanceExtractor.getDistance(dataMetadata, lines[i], lines[j]);
					if (distanceFound < Constants.CLUSTER_WIDTH) {
						minDistance = distanceFound;
						cluster.add(j);
						minFound = j;
						// logger.info(lines[j]);
					}
					else if (minDistance > distanceFound) {
						minDistance = distanceFound;
						minFound = j;
					}

				}
			}
			if (minDistance > Constants.CLUSTER_THRESHOLD) {
				logger.info("No Cluster found for  following entry as distance between nearest point and itself is "
						+ minDistance);
				logger.info(lines[i]);
				logger.info(lines[minFound]);
			}
			else {
				cluster.add(minFound);
				logger.info("cluster found of size:" + +cluster.size() + " => " + minDistance);
				logger.info(lines[i]);
				logger.info(lines[minFound]);

				clusters.add(cluster);
				logger.info("Cluster found : ");
			}
		}
	}

	/**
	 * This function extracts basic metadata from input data. It extracts
	 * frequency, min, max, mode and
	 * 
	 * @param headerLine
	 * @param numColumns
	 * @param dataStartCount
	 * @param lines
	 * @return
	 */
	private DataMetadata basicMetadata(String headerLine, int numColumns, int dataStartCount, String[] lines) {
		DataMetadata dataMetadata = new DataMetadata(numColumns);
		/**
		 * Extract column labels
		 */
		String[] headerLabels = headerLine.split(Constants.COMMA);
		/**
		 * Extract column types
		 */
		int numRangeColumn = 0;
		for (int i = dataStartCount; i < lines.length; i++) {

			// logger.info("Working on line " + i);
			String[] columnDataValue = lines[i].split(Constants.COMMA);
			Object[] row = new Object[headerLabels.length];
			for (int index = 0; index < columnDataValue.length; index++) {

				String value = columnDataValue[index].trim();
				if (value != Constants.IGNORE_CHAR) {
					ColumnMetadata metadata = dataMetadata.getColumn(index);
					if (metadata == null) {

						boolean isCurrancy = false;
						if (value.startsWith("$") || value.endsWith("$")) {
							isCurrancy = true;
						}
						if (value.matches(Constants.INT_REGEX)) {
							numRangeColumn++;
							metadata = new ColumnMetadata<Integer>(headerLabels[index], isCurrancy ? 'P' : 'i');
						}
						else if (value.matches(Constants.DOUBLE_REGEX)) {
							numRangeColumn++;
							metadata = new ColumnMetadata<Double>(headerLabels[index], isCurrancy ? '$' : 'd');
						}
						else {
							// logger.info(value + ":" + headerLabels[index]);
							metadata = new ColumnMetadata<String>(headerLabels[index], 's');
						}
						dataMetadata.setColumn(index, metadata);
					}
					Object entry = metadata.addEntryToMap(value);
					row[index] = entry;
					dataMetadata.setColumn(index, metadata);
				}
			}

			dataMetadata.addRow(row);
		}
		dataMetadata.setNumRangeColumns(numRangeColumn);
		for (ColumnMetadata<?> column : dataMetadata.columns) {
			column.setMinMaxAndMode();
		}
		return dataMetadata;
	}

	/**
	 * This method is entry point for
	 * 
	 * @param args
	 * @throws InvalidInputFileException
	 *             - if input is not in proper format.
	 */
	public static void main(String[] args) throws InvalidInputFileException {

		String headerLine = null;
		if (args.length == 2 && args[1] != null) {
			headerLine = args[1];
		}

		MetadataExtractor extractor = new MetadataExtractor();

		try {
			String fileData;
			fileData = FileUtils.readFileToString(new File(args[0]), "UTF-8");
			DataMetadata metadata = extractor.extractStatistics(fileData, headerLine);
		}
		catch (IOException e) {
			logger.info("Input file is not present on given path");
		}
		catch (Exception e) {
			e.printStackTrace();
		}
	}
}
