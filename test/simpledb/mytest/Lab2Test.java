package simpledb.mytest;

import org.junit.runner.RunWith;
import org.junit.runners.Suite;
import simpledb.*;

@RunWith(Suite.class)
@Suite.SuiteClasses({
        // Exercise 1
        PredicateTest.class,
        JoinPredicateTest.class,
        FilterTest.class,
        JoinTest.class,
        simpledb.systemtest.FilterTest.class,
        simpledb.systemtest.JoinTest.class,
        // Exercise 2
        IntegerAggregatorTest.class,
        StringAggregatorTest.class,
        AggregateTest.class,
        simpledb.systemtest.AggregateTest.class,
        // Exercise 3
        HeapPageWriteTest.class,
        HeapFileWriteTest.class,
        BufferPoolWriteTest.class,
        // Exercise 4
        InsertTest.class,
        simpledb.systemtest.InsertTest.class,
        simpledb.systemtest.DeleteTest.class
})
public class Lab2Test {

}
