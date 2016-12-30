package gtanonymization;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Set;

import org.apache.log4j.Logger;

import gtanonymization.domain.DataMetadata;

public class NaiveClusterExtractor {
	final static Logger logger = Logger.getLogger(NaiveClusterExtractor.class);


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
						int distanceFound = DistanceExtractor.getManhattanDistance(dataMetadata, lines[i], lines[j]);
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

					int distanceFound = DistanceExtractor.getManhattanDistance(dataMetadata, lines[i], lines[j]);
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

}
