package simpledb.mytest;

import org.junit.Assert;
import org.junit.Test;
import simpledb.common.Database;
import simpledb.common.Type;
import simpledb.execution.*;
import simpledb.storage.HeapFile;
import simpledb.storage.IntField;
import simpledb.storage.Tuple;
import simpledb.storage.TupleDesc;
import simpledb.systemtest.SimpleDbTestBase;
import simpledb.transaction.TransactionId;

import java.io.File;


public class MyLab2Test extends SimpleDbTestBase {


    @Test
    public void joinTest() {
        // construct a 3-column table schema
        Type types[] = new Type[]{Type.INT_TYPE, Type.INT_TYPE};
        String names[] = new String[]{"field0", "field1"};

        TupleDesc td = new TupleDesc(types, names);

        // create the tables, associate them with the data files
        // and tell the catalog about the schema  the tables.
        HeapFile table1 = new HeapFile(new File("data/file.dat"), td);
        Database.getCatalog().addTable(table1, "t1");

        HeapFile table2 = new HeapFile(new File("data/data.dat"), td);
        Database.getCatalog().addTable(table2, "t2");

        // construct the query: we use two SeqScans, which spoonfeed
        // tuples via iterators into join
        TransactionId tid = new TransactionId();

        SeqScan ss1 = new SeqScan(tid, table1.getId(), "t1");
        SeqScan ss2 = new SeqScan(tid, table2.getId(), "t2");

        // create a filter for the where condition
        Filter sf1 = new Filter(
                new Predicate(0,
                        Predicate.Op.LESS_THAN, new IntField(5)), ss1);

        JoinPredicate p = new JoinPredicate(1, Predicate.Op.EQUALS, 1);
        Join j = new Join(p, sf1, ss2);

        // and run it
        int[] ans = new int[]{2,20,2,20};
        int i=0;
        try {
            j.open();
            while (j.hasNext()) {
                Tuple tup = j.next();
                IntField f = (IntField) tup.getField(0);
                Assert.assertEquals(ans[i++], f.getValue());
            }
            j.close();
            Database.getBufferPool().transactionComplete(tid);

        } catch (Exception e) {
            e.printStackTrace();
        }

    }

}
