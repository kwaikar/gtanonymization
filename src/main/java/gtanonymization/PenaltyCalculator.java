package gtanonymization;

import java.util.List;

import gtanonymization.domain.ColumnStatistics;
import gtanonymization.domain.Row;

public class PenaltyCalculator {
	private ColumnStatistics[] columns = null;
	private Double weightOfQuasiIdentifierColumn = null;
	private int totalQuasiIdentifiers = 0;

	public PenaltyCalculator(ColumnStatistics[] columns) {
		super();
		this.columns = columns;
	}

	public double getTotalPenalty(List<List<Row>> equivalentClasses, int totalNumRows)
	{
		int count=0;
		double totalPenalty=0.0;
		for (List<Row> list : equivalentClasses) {
			count+=list.size();
			totalPenalty+=list.size() * getNormalizedCertainityPenalty(list.get(0));
		}
		totalPenalty= (double)totalPenalty/(totalQuasiIdentifiers*totalNumRows);
		System.out.println("Total count found "+count + " |  Penalty : "+totalPenalty);
		return totalPenalty;
	}
	
	/**
	 * NCP(G) = wi*NCPAi(G)
	 * 
	 * @param row
	 * @return
	 */
	public double getNormalizedCertainityPenalty(Row row) {

		double totalPenalty = 0.0;
		if (weightOfQuasiIdentifierColumn == null) {
			for (ColumnStatistics columnStatistics : columns) {
				if (columnStatistics.isQuasiIdentifier() && columnStatistics.getType() != 's') {
					totalQuasiIdentifiers++;
				}
			}
			weightOfQuasiIdentifierColumn = 1.0 / totalQuasiIdentifiers;
		}
		
		int index = 0;
		for (ColumnStatistics columnStatistics : columns) {
			if (columnStatistics.isQuasiIdentifier() && columnStatistics.getType() != 's') {
				totalPenalty += weightOfQuasiIdentifierColumn*getNormalizedCertainityPenalty(index, row);
			}
			index++;
		}
		System.out.println("Penalty Found"+totalPenalty);
		return totalPenalty;
	}

	/**
	 * NCPANum(G) = maxGANum − minGANum/ maxANum − minANum
	 * 
	 * @param columnId
	 * @param row
	 * @return
	 */
	private double getNormalizedCertainityPenalty(int columnId, Row row) {

		ColumnStatistics column = columns[columnId];
		switch (
			column.getType()
		) {

		
		case 'i':
		case 'P':
			System.out.println("Column"+column);
			return (double) ((Integer) row.getNewRow()[columnId].getRight()
					- (Integer) row.getNewRow()[columnId].getLeft())
					/ ((Integer) column.getMax() - (Integer) column.getMin());

		case 'd':
		case '$':
			return ((Double) row.getNewRow()[columnId].getRight() - (Double) row.getNewRow()[columnId].getLeft())
					/ ((Double) column.getMax() - (Double) column.getMin());
		default:
			/**
			 * For string, we are not doing any anonymization
			 */
			return 0.0;
		}
	}
}
