package gtanonymization;

import java.io.File;
import java.io.IOException;
import java.util.LinkedList;
import java.util.List;

import javax.xml.parsers.DocumentBuilder;

import org.apache.commons.io.FileUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.log4j.Logger;

import gtanonymization.domain.ColumnMetadata;
import gtanonymization.domain.ColumnStatistics;
import gtanonymization.domain.DataMetadata;
import gtanonymization.domain.Row;
import scala.collection.Iterator;
import scala.xml.Node;
import scala.xml.NodeSeq;
import scala.xml.XML;

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
		if (lines.length >= 1) {
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
		MetadataExporter exporter = new MetadataExporter();
		exporter.exportMetadata(dataMetadata, "/home/kanchan/metadata.xml", false);

		ColumnStatistics[] columns =this.extractMetadata("/home/kanchan/metadata.xml");
		MondrianMultiDSKanonymity mmdk = new MondrianMultiDSKanonymity(columns);
		List<Row> rows = new LinkedList<Row>();
		int index=0;
		for (Object[] rows2 : dataMetadata.rows) {
			Row row = new Row(rows2,columns);
			row.setId(index++);
			rows.add(row);
		}
		mmdk.anonymize(rows, 4,mmdk.isQuantitative);
		
		PenaltyCalculator pc = new PenaltyCalculator(columns);
		mmdk.destroy();
		  index=0;
		StringBuilder sb = new StringBuilder();

		StringBuilder output = new StringBuilder();
		for (Row row : rows) {
			sb.append("\n "+index);
			for (Object object : row.row) {
				sb.append(object+ " ");
			}
			sb.append("\n "+index++);
			int cnt=0;
			for (Object object : row.newRow) {
				sb.append(object+ " ");
				if(columns[cnt].isQuasiIdentifier())
				{
					output.append(object);
				}
				else
				{
					output.append(row.row[cnt]);
				}
				cnt++;
				if(cnt!=row.newRow.length)
				{
					output.append(",");
				}
				else
				{
					output.append("\n");
				}
			}
			sb.append("\n");
		}
		try {
			FileUtils.writeStringToFile(new File("/home/kanchan/output.csv"), output.toString());
		}
		catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		System.out.println(sb.toString());

		pc.getTotalPenalty(mmdk.getEquivalentClasses(), dataMetadata.rows.size());
		//logger.info(sb.toString());
		// Kmeans kmeans = new Kmeans();
		// int numClusters = lines.length / 8;
		// kmeans.trainModelAndPredict(dataMetadata, numClusters);
		// NaiveClusterExtractor nce = new NaiveClusterExtractor();
		// nce.extractClusters(dataStartCount, lines, dataMetadata);
		// LatticeCreator lc = new LatticeCreator();
		// lc.formTree(lines[dataStartCount].split(","), dataMetadata);
		logger.info("done!");
		return dataMetadata;
	}

	ColumnStatistics[] extractMetadata(String file) {

		DocumentBuilder builder;
		Node xml = XML.loadFile(file);
		NodeSeq seq = xml.$bslash$bslash("columns").$bslash("column");
		ColumnStatistics[] columns = new ColumnStatistics[seq.size()];
		Iterator<Node> itr = seq.iterator();
		int i=0;
		while (itr.hasNext()) {
			Node node = itr.next();
			System.out.println();
			ColumnStatistics column = new ColumnStatistics<Comparable>(node.$bslash("name").text(),
					node.$bslash("type").text().charAt(0),new Boolean( node.$bslash("isQuasiIdentifier").text()));
			column.setNumUniqueValues(Integer.parseInt(node.$bslash("num_unique").text()));
			if(column.getType()!='s' && (column.getType()=='P' ||column.getType()=='i') )
			{

				column.setMin(Integer.parseInt(node.$bslash("min").text()));
				column.setMax(Integer.parseInt(node.$bslash("max").text()));
			}else if (column.getType()=='$' ||column.getType()=='d') 
			{
				System.out.println(column);
				column.setMin(Double.parseDouble(node.$bslash("min").text()));
				column.setMax(Double.parseDouble(node.$bslash("max").text()));
			}
			System.out.println(column);
			
			columns[i++]=column;
		}

		return columns;
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
			System.out.println(headerLine);
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
