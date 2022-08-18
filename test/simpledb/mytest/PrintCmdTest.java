package simpledb.mytest;

import org.junit.Before;
import org.junit.Test;
import simpledb.SimpleDb;
import simpledb.common.Database;
import simpledb.common.Type;
import simpledb.execution.SeqScan;
import simpledb.storage.*;
import simpledb.systemtest.SimpleDbTestBase;
import simpledb.transaction.TransactionId;

import java.io.*;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;

public class PrintCmdTest extends SimpleDbTestBase {

    private Random random;
    private final int MIN_COL = 5; // must greater than 0
    private final int MIN_ROW = 10000; // must greater than 0
    private final int COL_OFFSET = 50;
    private final int ROW_OFFSET = 10000;
    private final int MAX_NUM = 10000;
    private Integer column;
    private Integer row;
    private List<List<Integer>> data;
    public void makeData(){
        this.data = new ArrayList<>();
        for (int i = 0; i < row; i++) {
            data.add(new ArrayList<>());
            for (int j = 0; j < column; j++) {
                data.get(i).add(random.nextInt(MAX_NUM));
            }
        }
        System.out.printf("data initialized！ row: %d, col: %d\n", row, column);
    }

    public void setRowAndColumn(int row, int column){
        this.row = row;
        this.column = column;
    }

    public void setRowAndColumn(){
        setRowAndColumn(random.nextInt(ROW_OFFSET) + MIN_ROW, random.nextInt(COL_OFFSET) + MIN_COL);
    }

    public void writeFile(){
        File file = new File(TXT_FILE_PATH);
        FileWriter fileWriter = null;
        BufferedWriter out = null;
        try {
            if (!file.exists()) {
                file.createNewFile();
            }
            fileWriter = new FileWriter(file, false);
            out = new BufferedWriter(fileWriter);
            for (int i = 0; i < row; i++) {
                for (int j = 0; j < column; j++) {
                    if (j > 0) {
                        out.write(",");
                    }
                    out.write(data.get(i).get(j).toString());
                }
                out.write("\r\n");
            }
            out.close();
            fileWriter.close();
            String args[] = new String[]{"convert", TXT_FILE_PATH, column.toString()};
            SimpleDb.main(args);
            file = new File(DAT_FILE_PATH);
            if (!file.exists()) {
                fail("create dat file failed");
            }
        } catch (Exception e) {
            e.printStackTrace();
            throw new RuntimeException(e);
        }
        System.out.println("file created!");
    }
    @Override
    @Before
    public void setUp() throws Exception {
        super.setUp();
        this.random = new Random();
        setRowAndColumn();
        makeData();
        writeFile();
    }

    @Test
    public void testSeqScanPrint(){
        for (int i = 1; i < 20000; i++) {
            for (int j = 1; j < 100; j++) {
                setRowAndColumn(i, j);
                makeData();
                writeFile();
                compareSeqScanAndData();
            }
        }
    }
    public void compareSeqScanAndData() {
        System.out.printf("start comparing, row [%d], col [%d]\n", row, column);
        // construct a 3-column table schema
        Type types[] = new Type[column];
        String names[] = new String[column];
        for (int i = 0; i < column; i++) {
            types[i] = Type.INT_TYPE;
            names[i] = "field" + i;
        }

        TupleDesc descriptor = new TupleDesc(types, names);

        // create the table, associate it with DAT_FILE_PATH
        // and tell the catalog about the schema of this table.
        HeapFile table1 = new HeapFile(new File(DAT_FILE_PATH), descriptor);
        Database.getCatalog().addTable(table1, FILE_NAME);

        // construct the query: we use a simple SeqScan, which spoonfeeds
        // tuples via its iterator.
        TransactionId tid = new TransactionId();
        SeqScan f = new SeqScan(tid, table1.getId());

        int i = 0;
        IntField field = null;
        try {
            // and run it
            f.open();
            while (f.hasNext()) {
                Tuple tup = f.next();
                for (int j = 0; j < column; j++) {
                    field = (IntField) tup.getField(j);
                    Integer expected = data.get(i).get(j);
                    Integer actual = field.getValue();
                    assertEquals(expected, actual);
                }
                i++;
            }
            assertEquals(i, row.intValue());
        } catch (Exception e) {
            System.out.println("Exception : " + e);
        } finally {
            f.close();
            Database.getBufferPool().transactionComplete(tid);
        }
    }

}
