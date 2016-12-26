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

import gtanonymization.domain.ColumnMetadata;
import gtanonymization.domain.DataMetadata;

/**
 * This class extracts metadata for each column from the input. It takes input
 * file,
 * for integer columns, extracts min, max and keeps counts for each column in
 * order to calculate probability for the same.
 * 
 * @author kanchan
 */
public class MetadataExtractor {

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
		System.out.println(dataMetadata);
		extractClusters(dataStartCount, lines, dataMetadata);
		System.out.println("done!");
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
				// System.out.println(lines[i]);
				for (int j = dataStartCount; j < lines.length; j++) {
					if (i != j && !isDisabled[j]) {
						int distanceFound = DistanceExtractor.getDistance(dataMetadata, lines[i], lines[j]);
						if (distanceFound < Constants.CLUSTER_WIDTH) {
							minDistance = distanceFound;
							isDisabled[j] = true;
							cluster.add(j);
							minFound = j;
							// System.out.println(lines[j]);
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
					System.out.println("Cluster found : " + cluster.size());
				}
				// System.out.println(lines[minFound]);
				// System.out.println(i + ":(" + minFound + "=>" + minDistance);
			}
		}
		System.out.println("Clusters found:" + clusters.size());
		/**
		 * Pass 2
		 */
		System.out.println("-------------------------Executing Pass 2----------------------------");
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
						// System.out.println(lines[j]);
					}
					else if (minDistance > distanceFound) {
						minDistance = distanceFound;
						minFound = j;
					}

					
				}
			}
			if (minDistance > Constants.CLUSTER_THRESHOLD) {
				System.out.println(
						"No Cluster found for  following entry as distance between nearest point and itself is "
								+ minDistance);
				System.out.println(lines[i]);
				System.out.println(lines[minFound]);
			}
			else {
				cluster.add(minFound);
				System.out.println(lines[i]);

				clusters.add(cluster);
				System.out.println("Cluster found : " + cluster.size());
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
		for (int i = dataStartCount; i < lines.length; i++) {

			// System.out.println("Working on line " + i);
			String[] columnDataValue = lines[i].split(Constants.COMMA);
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
							metadata = new ColumnMetadata<Integer>(headerLabels[index], isCurrancy ? 'P' : 'i');
						}
						else if (value.matches(Constants.DOUBLE_REGEX)) {
							metadata = new ColumnMetadata<Double>(headerLabels[index], isCurrancy ? '$' : 'd');
						}
						else {
							System.out.println(value + ":" + headerLabels[index]);
							metadata = new ColumnMetadata<String>(headerLabels[index], 's');
						}
						dataMetadata.setColumn(index, metadata);
					}
					value = value.trim().replaceAll("\\$", "");
					switch (
						metadata.getType()
						) {
					/**
					 * Add integer value
					 */
					case 'i':
						metadata.addEntryToMap(Integer.parseInt(value));
						break;

					/**
					 * Add double value
					 */
					case 'd':
						metadata.addEntryToMap(Double.parseDouble(value));
						break;
					/**
					 * add integer currency
					 */

					case 'P':
						metadata.addEntryToMap(Integer.parseInt(value));
						break;

					case '$':
						metadata.addEntryToMap(Double.parseDouble(value));
						break;

					case 's':
						metadata.addEntryToMap(value.trim());
						break;
					}

					dataMetadata.setColumn(index, metadata);
				}
			}
		}
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
			extractor.extractStatistics(fileData, headerLine);
		}
		catch (IOException e) {
			System.out.println("Input file is not present on given path");
		}
		catch (Exception e) {
			e.printStackTrace();
		}
	}
}
