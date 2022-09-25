package simpledb.mytest;

import org.junit.Before;
import org.junit.Test;
import simpledb.SimpleDb;
import simpledb.common.Database;
import simpledb.common.DbException;
import simpledb.common.Type;
import simpledb.execution.SeqScan;
import simpledb.storage.*;
import simpledb.systemtest.SimpleDbTestBase;
import simpledb.transaction.TransactionAbortedException;
import simpledb.transaction.TransactionId;

import java.io.*;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;
import java.util.concurrent.*;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;

public class PrintCmdTest extends SimpleDbTestBase {

    private static final String DATA_FILE_NAME = "printCmdTest";

    private static final int MAX_NUM = 100000;
    private static final ThreadPoolExecutor executor;

    static {
        int corePoolSize = 150, maxPoolSize = 300, keepAliveSeconds = 600, queueCapacity = 25;
        final BlockingQueue<Runnable> blockingQueue = new LinkedBlockingQueue(queueCapacity);
        final ThreadFactory threadFactory = r -> new Thread(r, "print-cmd-test-task-pool-" + r.hashCode());
        final RejectedExecutionHandler rejectedExecutionHandler = new ThreadPoolExecutor.CallerRunsPolicy();
        executor = new ThreadPoolExecutor(corePoolSize, maxPoolSize, (long) keepAliveSeconds,
                TimeUnit.SECONDS, blockingQueue, threadFactory, rejectedExecutionHandler);
    }

    @Test
    public void testSeqScanPrint() {
        List<Tester> testerList = new ArrayList<>();
        List<Future<?>> futures = new ArrayList<>();
        int minRow = 5500;
        int maxRow = 5501, maxCol = 10;
        for (int i = minRow; i <= maxRow; i++) {
            for (int j = 1; j <= maxCol; j++) {
                testerList.add(new Tester(new File(getTxtFilePath(String.format("%s-%d-%d", DATA_FILE_NAME, i, j))),
                        new File(getDATFilePath(String.format("%s-%d-%d", DATA_FILE_NAME, i, j))),
                        i, j, MAX_NUM));
            }
        }

        int partitionSize = 100000;
        List<List<Tester>> subLists = new ArrayList<>();
        for (int i = 0; i < testerList.size(); i += partitionSize) {
            subLists.add(testerList.subList(i, Math.min(i + partitionSize, testerList.size())));
        }

        for (List<Tester> partial : subLists) {
            futures.add(executor.submit(() -> partial.forEach(t -> t.start())));
        }

        futures.forEach(r -> {
            try {
                r.get();
            } catch (InterruptedException | ExecutionException e) {
                throw new RuntimeException(e);
            }
        });
    }

    class Tester {
        private final File txtFile;
        private final File datFile;
        private final Random random;
        private final Integer row;
        private final Integer col;
        private final Integer maxNum;
        private final List<List<Integer>> data;

        public Tester(File txtFile, File datFile, int row, int col, int maxNum) {
            this.txtFile = txtFile;
            this.datFile = datFile;
            this.random = new Random();
            this.row = row;
            this.col = col;
            this.maxNum = maxNum;
            this.data = new ArrayList<>();
            for (int i = 0; i < row; i++) {
                data.add(new ArrayList<>());
                for (int j = 0; j < col; j++) {
                    data.get(i).add(random.nextInt(maxNum));
                }
            }
        }

        private void createFiles() throws IOException, DbException, TransactionAbortedException {
            // 1. create txt file
            if (!txtFile.exists()) {
                txtFile.createNewFile();
            }

            try (FileWriter fileWriter = new FileWriter(txtFile, false);
                 BufferedWriter out = new BufferedWriter(fileWriter)) {
                for (int i = 0; i < row; i++) {
                    for (int j = 0; j < col; j++) {
                        if (j > 0) {
                            out.write(",");
                        }
                        out.write(data.get(i).get(j).toString());
                    }
                    out.write("\r\n");
                    out.flush();
                }
            } catch (IOException e) {
                throw e;
            }

            String args[] = new String[]{"convert", txtFile.getAbsolutePath(), col.toString()};
            try {
                SimpleDb.main(args);
            } catch (DbException | TransactionAbortedException e) {
                throw e;
            }

            if (!datFile.exists()) {
                throw new IOException("create txt file failed: " + txtFile.getAbsolutePath());
            }
        }

        private void deleteFiles() {
            if (txtFile.exists()) {
                txtFile.delete();
            }
            if (datFile.exists()) {
                datFile.delete();
            }
        }

        private void compare() {
            // construct a 3-column table schema
            Type types[] = new Type[col];
            String names[] = new String[col];
            for (int i = 0; i < col; i++) {
                types[i] = Type.INT_TYPE;
                names[i] = "field" + i;
            }

            TupleDesc descriptor = new TupleDesc(types, names);

            // create the table, associate it with DAT_FILE_PATH
            // and tell the catalog about the schema of this table.
            HeapFile table1 = new HeapFile(datFile, descriptor);
            Database.getCatalog().addTable(table1, datFile.getName());
//            System.out.println(String.format("add [%d] [%s]", table1.getId(), datFile.getName()) );


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
                    for (int j = 0; j < col; j++) {
                        field = (IntField) tup.getField(j);
                        Integer expected = data.get(i).get(j);
                        Integer actual = field.getValue();
                        assertEquals(expected, actual);
                    }
                    i++;
                }
                assertEquals(i, row.intValue());
            } catch (Exception e) {
                throw new RuntimeException(e);
            } finally {
                f.close();
                Database.getBufferPool().transactionComplete(tid);
            }
        }

        public void start() {
            try {
                createFiles();
                compare();
            } catch (Exception e) {
                throw new RuntimeException(e);
            } finally {
                deleteFiles();
            }
        }
    }

}
