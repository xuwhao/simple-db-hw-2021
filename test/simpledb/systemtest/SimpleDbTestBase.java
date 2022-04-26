package simpledb.systemtest;

import org.junit.Before;

import simpledb.common.Database;

/**
 * Base class for all SimpleDb test classes. 
 * @author nizam
 *
 */
public class SimpleDbTestBase {

	public final String FILE_NAME = "file";
	public final String TXT_FILE_PATH = "/root/src/simple-db-hw-2021/data/" + FILE_NAME + ".txt";

	public final String DAT_FILE_PATH = "/root/src/simple-db-hw-2021/data/" + FILE_NAME + ".dat";

	/**
	 * Reset the database before each test is run.
	 */
	@Before	public void setUp() throws Exception {					
		Database.reset();
	}
	
}
