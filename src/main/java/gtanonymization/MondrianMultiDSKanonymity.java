/**
 * 
 */
package gtanonymization;

import java.util.Collections;
import java.util.Comparator;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Set;

import gtanonymization.domain.ColumnStatistics;
import gtanonymization.domain.Row;

/**
 * This class implements mondrian multi-dimensional k-anonymity.
 * 
 * @author kanchan
 */
public class MondrianMultiDSKanonymity {

	/**
	 * This method accepts rows, Column Heuristics and
	 * 
	 * @param rows
	 * @param k
	 * @return
	 */
	ColumnStatistics[] columns = null;

	public MondrianMultiDSKanonymity(ColumnStatistics[] columns) {
		this.columns = columns;
	}

	public  void anonymize(List<Row> rows, int k) {
		if (rows.size() < k) {
			System.out.println("No cut allowed. Number of rows present");
		}
		else if (rows.size() == k) {
			System.out.println("Cluster cannot be paritioned further.");
		}

		final int dim = selectDimension(rows);
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
		
		int i=0;
		List<Row> leftSet = new LinkedList<Row>();
		List<Row> rightSet = new LinkedList<Row>();
		while (i<=(rows.size()/2))
		{
			leftSet.add(rows.get(i));
			i++;
		}
		while(i<=rows.size())
		{
			rightSet.add(rows.get(i));
			i++;
		}
		anonymize(leftSet, k);
		anonymize(rightSet, k);

	}

	/**
	 * The dimension needs to be selected based on the column which has maximum
	 * range, i.e. unique values.
	 * 
	 * @param rows
	 * @return
	 */
	public int selectDimension(List<Row> rows) {
		int index = 0;
		int maxColumn = -1;
		int maxValue = 0;

		for (ColumnStatistics<?> column : columns) {
			Set values = new HashSet();
			if (column.getType() != 's') {
				for (Row row : rows) {

					switch (
						column.getType()
					) {

					case 'i':
					case 'P':
						values.add((Integer) row.row[index]);

						break;
					case 'd':
					case '$':
						values.add((Double) row.row[index]);
						break;
					default:
						break;
					}
				}
				if (values.size() > maxValue) {
					maxValue = values.size();
					maxColumn = index;
				}
			}
			index++;
		}
		System.out.println("MaxColumn found : " + maxColumn + " : " + maxValue);
		return maxColumn;
	}
}
