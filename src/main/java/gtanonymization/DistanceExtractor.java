package gtanonymization;

import gtanonymization.domain.ColumnMetadata;
import gtanonymization.domain.DataMetadata;

public class DistanceExtractor {

	public static int getDistance(DataMetadata lineMetadata, String line1String, String line2String) {
		String[] line1 = line1String.split(",");
		String[] line2 = line2String.split(",");
		int index = 0;
		int totalDistance = 0;
		for (ColumnMetadata<Comparable> column : lineMetadata.columns) {

			String value1 = line1[index].trim().replaceAll("\\$", "").replaceAll("\\?", "");
			String value2 = line2[index++].trim().replaceAll("\\$", "").replaceAll("\\?", "");
			if (value1.length()!=0 && value2.length()!=0)
			{
			switch (
				column.getType()
			) {

			/**
			 * Add integer value
			 */
			case 'i':
			case 'P':
				totalDistance += (double)(100*((double)Math.abs(Integer.parseInt(value1)-Integer.parseInt(value2))/column.getRange()));
				break;

			/**
			 * Add double value
			 */
			case 'd':
			case '$':
				totalDistance +=(double)(100*((double)Math.abs(Double.parseDouble(value1)-Double.parseDouble(value2))/column.getRange())); 
				break;
				/**
				 * add integer currency
				 */
			case 's':
				/**
				 * add String
				 */
				if (value1.equalsIgnoreCase(value2)) {
					totalDistance += Constants.DISTANCE_SAME;
				}
				else {
					totalDistance += Constants.DISTANCE_OPP;
				}
				break;
			}
			}
			else
			{
				totalDistance+=Constants.DISTANCE_OPP;
			}
		}

		return totalDistance;

	}
	public static void main(String[] args) {
		
		String value1="7";
		String value2="14";
		System.out.println((int)(100*((double)Math.abs(Integer.parseInt(value1)-Integer.parseInt(value2))/15)));
	}
}
