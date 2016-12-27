package gtanonymization;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import org.apache.log4j.Logger;

import gtanonymization.domain.DataMetadata;

public class LatticeCreator {

	final static Logger logger = Logger.getLogger(LatticeCreator.class);
	private class LatticeSingleColumn implements Comparable<LatticeSingleColumn> {

		/* (non-Javadoc)
		 * @see java.lang.Object#toString()
		 */
		@Override
		public String toString() {
			return "LatticeSingleColumn [probability=" + probability + ", column=" + column +",value="+value+"]";
		}

		private Double probability;
		private int columnId;
		private String column;
		private String value;

		private double risk;
		/**
		 * @return the probability
		 */
		public double getProbability() {
			return probability;
		}

		/**
		 * @return the importance
		 */
		public double getRisk() {
			return risk;
		}

		/**
		 * @return the columnId
		 */
		public int getColumnId() {
			return columnId;
		}

		public LatticeSingleColumn(double probability, int columnId, String columnName,String value) {
			super();
			this.probability = probability;
			this.columnId = columnId;
			this.column = columnName;
			this.value=value;
			this.risk=1-probability;
		}

		/*
		 * (non-Javadoc)
		 * @see java.lang.Comparable#compareTo(java.lang.Object)
		 */
		public int compareTo(LatticeSingleColumn o) {
			// TODO Auto-generated method stub
			return (int) o.probability.compareTo(this.probability);
		}

	}

	public void formTree(String[] line, DataMetadata dataMetadata) {

		/**
		 * Sort columns by probability. 
		 */
		List<LatticeSingleColumn> columns = new ArrayList<LatticeSingleColumn>();
		for (int index = 0; index < line.length; index++) {
			LatticeSingleColumn le = new LatticeSingleColumn(dataMetadata.getColumn(index).getEntryFromMap(line[index]),
					index, dataMetadata.getColumn(index).getColumnName(),line[index]);
			columns.add(le);
		}

		int totalColumnsAvailable= line.length;
		int numberOfStrategiesAvailable = (int) Math.pow(2,totalColumnsAvailable);
		
		System.out.println("Number of strategies available"+numberOfStrategiesAvailable);
		System.out.println("Number of strategies with 3 layers for Integer columns"+(int)(Math.pow(2,(totalColumnsAvailable-dataMetadata.numRangeColumns))+Math.pow(3,dataMetadata.numRangeColumns)));
				
		int publisher_benefit=100*totalColumnsAvailable;
		int adversary_cost=totalColumnsAvailable;
		
		
		
		 
		int Level_0_utility=100; // When actual value is provided to the user.
		/**int Level_1_utility=90; // To be used in future once 2 layer implementation is done. - for range [0-3, 4-8] etc. **/
		int Level_2_utility=80; //value is kept as * 

		int Level_0_risk=-1; // will be taken as column risk; // When actual value is provided to the user.
		/**int Level_1_utility=90;  To be used in future once 2 layer implementation is done. - for range [0-3, 4-8] etc. **/
		int Level_2_risk=80; //value is kept as * 
		
		
		
		int publisher_cost=10*totalColumnsAvailable;
		int adv_benefit=200; 
		double identificationProbability=0;

		Collections.sort(columns);
		logger.info(columns);
	}

}
