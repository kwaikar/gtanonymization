package gtanonymization;

import gtanonymization.domain.ColumnMetadata;
import gtanonymization.domain.DataMetadata;
/**
 * This class provides methods for calculating distance between two string vectors.
 * @author kanchan
 *
 */
public class DistanceExtractor {

	/**
	 * To be used for calculating Manhattan distance between two vectors
	 * line1String and line2String.
	 * 
	 * @param dataMetadata
	 * @param line1String
	 * @param line2String
	 * @return
	 */
	public static int getManhattanDistance(DataMetadata dataMetadata, String line1String, String line2String) {
		return getDistance(dataMetadata, line1String, line2String, false);
	}

	/**
	 * To be used for calculating Euclidian distance between two value vectors
	 * line1String and line2String.
	 * 
	 * @param dataMetadata
	 * @param line1String
	 * @param line2String
	 * @return
	 */
	public static int getEuclidianDistance(DataMetadata dataMetadata, String line1String, String line2String) {
		return getDistance(dataMetadata, line1String, line2String, false);
	}

	/**
	 * This method currently supports only two distance measures, Euclidian and
	 * manhattan
	 * 
	 * @param dataMetadata
	 * @param line1String
	 * @param line2String
	 * @param isEuclidian
	 * @return
	 */
	private static int getDistance(DataMetadata dataMetadata, String line1String, String line2String,
			boolean isEuclidian) {
		String[] line1 = line1String.split(",");
		String[] line2 = line2String.split(",");
		int index = 0;
		int totalDistance = 0;
		for (ColumnMetadata<Comparable> column : dataMetadata.columns) {
			int distance = 0;

			String value1 = line1[index].trim().replaceAll("\\$", "").replaceAll("\\?", "");
			String value2 = line2[index++].trim().replaceAll("\\$", "").replaceAll("\\?", "");
			if (value1.length() != 0 && value2.length() != 0) {
				switch (
					column.getType()
				) {

				/**
				 * Add integer value
				 */
				case 'i':
				case 'P':
					distance += (double) (100 * ((double) Math.abs(Integer.parseInt(value1) - Integer.parseInt(value2))
							/ column.getRange()));
					break;

				/**
				 * Add double value
				 */
				case 'd':
				case '$':
					distance += (double) (100
							* ((double) Math.abs(Double.parseDouble(value1) - Double.parseDouble(value2))
									/ column.getRange()));
					break;
				/**
				 * add integer currency
				 */
				case 's':
					/**
					 * add String
					 */
					if (value1.equalsIgnoreCase(value2)) {
						distance += Constants.DISTANCE_SAME;
					}
					else {
						distance += Constants.DISTANCE_OPP;
					}
					break;
				}
			}
			else {
				/**
				 * Value is unknwon - in worst case, the distance would be max.
				 */
				distance += Constants.DISTANCE_OPP;
			}
			if (isEuclidian) {
				totalDistance += distance * distance;
			}
			else {

				totalDistance += distance;
			}
		}

		return (int) (isEuclidian ? Math.sqrt(totalDistance) : totalDistance);

	}
}
