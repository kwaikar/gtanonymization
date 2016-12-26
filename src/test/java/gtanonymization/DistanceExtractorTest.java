package gtanonymization;

import org.junit.Assert;
import org.junit.Test;

public class DistanceExtractorTest  {

	@Test
	public void testDistanceExtractor()
	{
		String value1="7";
		String value2="14";
		Assert.assertEquals(46,(int)(100*((double)Math.abs(Integer.parseInt(value1)-Integer.parseInt(value2))/15)));

		
	}
}
