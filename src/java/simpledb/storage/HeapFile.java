package simpledb.storage;

import simpledb.common.*;
import simpledb.index.BTreeFile;
import simpledb.index.BTreeLeafPage;
import simpledb.index.BTreePageId;
import simpledb.index.BTreeRootPtrPage;
import simpledb.transaction.TransactionAbortedException;
import simpledb.transaction.TransactionId;

import java.io.*;
import java.util.*;

/**
 * HeapFile is an implementation of a DbFile that stores a collection of tuples
 * in no particular order. Tuples are stored on pages, each of which is a fixed
 * size, and the file is simply a collection of those pages. HeapFile works
 * closely with HeapPage. The format of HeapPages is described in the HeapPage
 * constructor.
 * 
 * @see HeapPage#HeapPage
 * @author Sam Madden
 */
public class HeapFile implements DbFile {

    private File file;

    private TupleDesc tupleDesc;
    /**
     * Constructs a heap file backed by the specified file.
     * 
     * @param f
     *            the file that stores the on-disk backing store for this heap
     *            file.
     */
    public HeapFile(File f, TupleDesc td) {
        this.file = f;
        this.tupleDesc = td;
    }

    /**
     * Returns the File backing this HeapFile on disk.
     * 
     * @return the File backing this HeapFile on disk.
     */
    public File getFile() {
        return this.file;
    }

    /**
     * Returns an ID uniquely identifying this HeapFile. Implementation note:
     * you will need to generate this tableid somewhere to ensure that each
     * HeapFile has a "unique id," and that you always return the same value for
     * a particular HeapFile. We suggest hashing the absolute file name of the
     * file underlying the heapfile, i.e. f.getAbsoluteFile().hashCode().
     * 
     * @return an ID uniquely identifying this HeapFile.
     */
    public int getId() {
        return this.file.getAbsoluteFile().hashCode();
    }

    /**
     * Returns the TupleDesc of the table stored in this DbFile.
     * 
     * @return TupleDesc of this DbFile.
     */
    public TupleDesc getTupleDesc() {
        return this.tupleDesc;
    }

    // see DbFile.java for javadocs
    public Page readPage(PageId pid) {
        RandomAccessFile raf = null;
        HeapPage heapPage = null;
        byte data[] = new byte[BufferPool.getPageSize()];
        try {
            raf = new RandomAccessFile(this.file, "r");
            int n = raf.read(data, pid.getPageNumber(), data.length);
            if (n  == -1){
                throw new IOException("read page failed, reach the end of the file!");
            }
            heapPage = new HeapPage((HeapPageId) pid, data);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }

        return heapPage;
    }

    // see DbFile.java for javadocs
    public void writePage(Page page) throws IOException {
        // some code goes here
        // not necessary for lab1
    }

    /**
     * Returns the number of pages in this HeapFile.
     */
    public int numPages() {
        return (int)Math.ceil(this.file.length() * 1.0/ BufferPool.getPageSize());
    }

    // see DbFile.java for javadocs
    public List<Page> insertTuple(TransactionId tid, Tuple t)
            throws DbException, IOException, TransactionAbortedException {
        // some code goes here
        return null;
        // not necessary for lab1
    }

    // see DbFile.java for javadocs
    public ArrayList<Page> deleteTuple(TransactionId tid, Tuple t) throws DbException,
            TransactionAbortedException {
        // some code goes here
        return null;
        // not necessary for lab1
    }

    // see DbFile.java for javadocs
    public DbFileIterator iterator(TransactionId tid) {
        return new HeapFileIterator(this, tid);
    }

}

class HeapFileIterator extends AbstractDbFileIterator {

    Iterator<Tuple> it = null;
    HeapPage heapPage = null;

    final TransactionId tid;
    final HeapFile f;

    /**
     * Constructor for this iterator
     * @param f - the HeapFile containing the tuples
     * @param tid - the transaction id
     */
    public HeapFileIterator(HeapFile f, TransactionId tid) {
        this.f = f;
        this.tid = tid;
    }

    /**
     * Open this iterator by getting an iterator on the first page
     */
    public void open() throws DbException, TransactionAbortedException {
        PageId pid = new HeapPageId(this.f.getId(), 0);
        this.heapPage = (HeapPage) Database.getBufferPool().getPage(tid, pid, Permissions.READ_ONLY);
        it = this.heapPage.iterator();
    }

    /**
     * Read the next tuple either from the current page if it has more tuples or
     * from the next page by following the right sibling pointer.
     *
     * @return the next tuple, or null if none exists
     */
    @Override
    protected Tuple readNext() throws TransactionAbortedException, DbException {
        if (it != null && !it.hasNext())
            it = null;

        while (it == null && heapPage != null) {
            HeapPageId nextp = null;
            int pgNo = heapPage.pid.getPageNumber() + BufferPool.getPageSize();
            int pgNum = (int)Math.ceil(pgNo * 1.0/ BufferPool.getPageSize()) + 1;
            if(pgNum <= this.f.numPages()){
                nextp = new HeapPageId(heapPage.pid.getTableId(), pgNo);
            }
            if(nextp == null) {
                heapPage = null;
            }
            else {
                heapPage = (HeapPage) Database.getBufferPool().getPage(tid, nextp, Permissions.READ_WRITE);
                it = heapPage.iterator();
                if (!it.hasNext())
                    it = null;
            }
        }

        if (it == null)
            return null;
        return it.next();
    }

    /**
     * rewind this iterator back to the beginning of the tuples
     */
    public void rewind() throws DbException, TransactionAbortedException {
        close();
        open();
    }

    /**
     * close the iterator
     */
    public void close() {
        super.close();
        it = null;
        heapPage = null;
    }
}
