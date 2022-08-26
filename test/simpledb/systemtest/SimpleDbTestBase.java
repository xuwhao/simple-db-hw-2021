package simpledb.systemtest;

import org.junit.Before;

import simpledb.common.Database;

/**
 * Base class for all SimpleDb test classes. 
 * @author nizam
 *
 */
public class SimpleDbTestBase {

	public static final String TXT_FILE_PREFIX = "/root/src/simple-db-hw-2021/data/";

	public static final String DAT_FILE_PREFIX = "/root/src/simple-db-hw-2021/data/";

	/**
	 * Reset the database before each test is run.
	 */
	@Before	public void setUp() throws Exception {					
		Database.reset();
	}

	public static String getTxtFilePath(final String fileName){
		return TXT_FILE_PREFIX + fileName + ".txt";
	}

	public static String getDATFilePath(final String fileName){
		return DAT_FILE_PREFIX + fileName + ".dat";
	}

	
}
