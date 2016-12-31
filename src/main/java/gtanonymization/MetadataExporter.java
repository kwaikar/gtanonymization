package gtanonymization;

import java.io.File;
import java.io.IOException;
import java.text.DecimalFormat;
import java.util.Iterator;

import org.apache.commons.io.FileUtils;
import org.apache.commons.lang3.tuple.ImmutablePair;
import org.apache.commons.lang3.tuple.Pair;

import gtanonymization.domain.ColumnMetadata;
import gtanonymization.domain.DataMetadata;
import gtanonymization.domain.ValueMetadata;

/**
 * This class is used for exporting metadata into an xml.
 * 
 * @author kanchan
 */
public class MetadataExporter {

	static final DecimalFormat decimalFormat = new DecimalFormat("#.######");

	/**
	 * This function creates a metadata.xml containing extracted hierarchy and
	 * probabilities found.
	 * 
	 * @param dataMetadata
	 */
	public void exportMetadata(DataMetadata dataMetadata, String filePath, Boolean printRanges) {
		StringBuilder sb = new StringBuilder();
		sb.append("<columns>");
		for (ColumnMetadata column : dataMetadata.columns) {
			sb.append("<column>");
			sb.append("<name>" + column.getColumnName() + "</name>");
			sb.append("<type>" + column.getType() + "</type>");
			sb.append("<num_unique>" + column.getNumUniqueValues() + "</num_unique>");
			sb.append("<ranges>");
			if (!printRanges) {
				Iterator<ValueMetadata> itr = column.getValues().iterator();
				while (itr.hasNext()) {
					ValueMetadata value = itr.next();

					sb.append("<range>");
					sb.append("<value>" + value.getValue() + "</value>");
					sb.append("<frequency>" + value.getCount() + "</frequency>");
					sb.append("<probability>" + decimalFormat.format(value.getProbability()) + "</probability>");
					sb.append("</range>");
				}
			}
			else {
				switch (
					column.getType()
				) {

				case 'i':
				case 'P':
					int singleRange = (int) getIncrementBasedOnRangesCalculated(column);

					for (Integer i = (Integer) column.getMin(); i < (Integer) column.getMax();) {
						Pair<Integer, Integer> p = new ImmutablePair<Integer, Integer>(i,
								(Integer) ((i + singleRange) > (Integer) column.getMax() ? column.getMax()
										: (i + singleRange)));
						int count = 0;
						Iterator<ValueMetadata<Integer>> itr = column.getValues().iterator();
						while (itr.hasNext()) {
							ValueMetadata<Integer> value = itr.next();
							if (value.getValue() >= p.getLeft() && value.getValue() <= p.getRight()) {
								count += value.getCount();
							}
						}
						sb.append(printRange(dataMetadata, p, count));
						i = i + singleRange + 1;
					}
					break;
				case 'd':
				case '$':
					double increment = getIncrementBasedOnRangesCalculated(column);

					for (double i = (Double) column.getMin(); i < (Double) column.getMax();) {
						Pair<Double, Double> p = new ImmutablePair<Double, Double>(i,
								(Double) ((i + increment) > (Double) column.getMax() ? column.getMax()
										: (i + increment)));
						int count = 0;
						Iterator<ValueMetadata<Double>> itr = column.getValues().iterator();
						while (itr.hasNext()) {
							ValueMetadata<Double> value = itr.next();
							if (value.getValue() >= p.getLeft() && value.getValue() <= p.getRight()) {
								count += value.getCount();
							}
						}
						sb.append(printRange(dataMetadata, p, count));
						i = i + increment + 1;
					}
					break;
				/**
				 * add integer currency
				 */

				case 's':
					if (printRanges) {
						Iterator<ValueMetadata> itr = column.getValues().iterator();
						while (itr.hasNext()) {
							ValueMetadata<String> value = itr.next();

							sb.append("<range>");
							sb.append("<value>" + value.getValue() + "</value>");
							sb.append("<frequency>" + value.getCount() + "</frequency>");
							sb.append("<probability>" + decimalFormat.format(value.getProbability()) + "</probability>");
							sb.append("</range>");
						}
					}
					break;

				}
			}
			sb.append("</ranges>");
			sb.append("</column>");
		}
		sb.append("</columns>");
		try

		{
			FileUtils.writeStringToFile(new File(filePath), sb.toString());
		}
		catch (IOException e) {
			e.printStackTrace();
		}
	}

	/**
	 * This method prints range in a String.
	 * 
	 * @param dataMetadata
	 * @param p
	 * @param count
	 * @return
	 */
	private String printRange(DataMetadata dataMetadata, Pair p, int count) {
		StringBuilder sb = new StringBuilder();
		sb.append("<range>");
		sb.append("<start>" + p.getLeft() + "</start>");
		sb.append("<end>" + p.getRight() + "</end>");
		sb.append("<frequency>" + count + "</frequency>");
		sb.append(
				"<probability>" + decimalFormat.format(((double) count / dataMetadata.rows.size())) + "</probability>");
		sb.append("</range>");
		return sb.toString();
	}

	/**
	 * This method checks min, max and returns number of intervals applicable.
	 * 
	 * @param column
	 * @return
	 */
	private double getIncrementBasedOnRangesCalculated(ColumnMetadata column) {
		int numIntervals = 0;
		if (column.getRange() > 100) {
			numIntervals = 50;
		}
		else if (column.getRange() < numIntervals && column.getRange() > (numIntervals / 2)) {
			numIntervals = 50;
		}
		else if (column.getRange() < (numIntervals / 2) && column.getRange() > (numIntervals / 4)) {
			numIntervals = 25;
		}

		else if (column.getRange() < (numIntervals / 4) && column.getRange() > (numIntervals / 8)) {
			numIntervals = 12;
		}

		else if (column.getRange() < (numIntervals / 8) && column.getRange() > (numIntervals / 16)) {
			numIntervals = 6;
		}
		else {
			numIntervals = 2;
		}
		return column.getRange() / numIntervals;
	}
}
