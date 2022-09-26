package simpledb.mytest;

import org.junit.runner.RunWith;
import org.junit.runners.Suite;
import simpledb.*;

@RunWith(Suite.class)
@Suite.SuiteClasses({
        // Exercise 1
        TupleTest.class,
        TupleDescTest.class,
        // Exercise 2
        CatalogTest.class,
        // Exercise 4
        HeapPageIdTest.class,
        RecordIdTest.class,
        HeapPageReadTest.class,
        // Exercise 5
        HeapFileReadTest.class,
        // Exercise 6
        simpledb.systemtest.ScanTest.class,
        MyLab1Test.class
})
public class Lab1Test {
}
