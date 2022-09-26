package simpledb.mytest;

import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Suite;
import simpledb.*;
import simpledb.common.Database;
import simpledb.common.Type;
import simpledb.execution.*;
import simpledb.storage.HeapFile;
import simpledb.storage.IntField;
import simpledb.storage.Tuple;
import simpledb.storage.TupleDesc;
import simpledb.transaction.TransactionId;

import java.io.File;

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
        simpledb.systemtest.DeleteTest.class,
        simpledb.systemtest.EvictionTest.class,
        // Exercise 5
        MyLab2Test.class
})
public class Lab2Test {
}
