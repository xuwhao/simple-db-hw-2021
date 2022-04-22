package simpledb.common;

import simpledb.common.Type;
import simpledb.storage.DbFile;
import simpledb.storage.HeapFile;
import simpledb.storage.TupleDesc;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;

/**
 * The Catalog keeps track of all available tables in the database and their
 * associated schemas.
 * For now, this is a stub catalog that must be populated with tables by a
 * user program before it can be used -- eventually, this should be converted
 * to a catalog that reads a catalog table from disk.
 *
 * @Threadsafe
 */
public class Catalog {

    private List<Integer> tableIds;
    private List<DbFile> dbFiles;
    private List<String> names;
    private List<String> pkeyFields;

    /**
     * Constructor.
     * Creates a new, empty catalog.
     */
    public Catalog() {
        this.tableIds = new ArrayList<>();
        this.dbFiles = new ArrayList<>();
        this.names = new ArrayList<>();
        this.pkeyFields = new ArrayList<>();
    }

    /**
     * Add a new table to the catalog.
     * This table's contents are stored in the specified DbFile.
     *
     * @param file      the contents of the table to add;  file.getId() is the identfier of
     *                  this file/tupledesc param for the calls getTupleDesc and getFile
     * @param name      the name of the table -- may be an empty string.  May not be null.  If a name
     *                  conflict exists, use the last table to be added as the table for a given name.
     * @param pkeyField the name of the primary key field
     */
    public void addTable(DbFile file, String name, String pkeyField) {
        int idx = -1;

        for (int i = 0; i < dbFiles.size(); i++) {
            if(dbFiles.get(i).getId() == file.getId()){
                idx = i;
            }
        }

        if (idx >= 0){
            this.tableIds.add(idx, file.getId());
            this.dbFiles.add(idx, file);
            this.names.add(idx, name);
            this.pkeyFields.add(idx, pkeyField);
            return;
        }

        for (int i = 0; i < names.size(); i++) {
            if(name!=null && name.equals(names.get(i))){
                idx = i;
            }
        }

        if(idx>=0){
            this.tableIds.add(idx, file.getId());
            this.dbFiles.add(idx, file);
            this.names.add(idx, name);
            this.pkeyFields.add(idx, pkeyField);
            return;
        }

        this.tableIds.add(file.getId());
        this.dbFiles.add(file);
        this.names.add(name);
        this.pkeyFields.add(pkeyField);
    }

    public void addTable(DbFile file, String name) {
        addTable(file, name, "");
    }

    /**
     * Add a new table to the catalog.
     * This table has tuples formatted using the specified TupleDesc and its
     * contents are stored in the specified DbFile.
     *
     * @param file the contents of the table to add;  file.getId() is the identfier of
     *             this file/tupledesc param for the calls getTupleDesc and getFile
     */
    public void addTable(DbFile file) {
        addTable(file, (UUID.randomUUID()).toString());
    }

    /**
     * Return the id of the table with a specified name,
     *
     * @throws NoSuchElementException if the table doesn't exist
     */
    public int getTableId(String name) throws NoSuchElementException {
        for (int i = 0; i < names.size(); i++) {
            if(name!=null && name.equals(names.get(i))){
                return tableIds.get(i);
            }
        }
        throw new NoSuchElementException("Catalog.class, func: getTableId, no such name: " + name);
    }

    /**
     * Returns the tuple descriptor (schema) of the specified table
     *
     * @param tableid The id of the table, as specified by the DbFile.getId()
     *                function passed to addTable
     * @throws NoSuchElementException if the table doesn't exist
     */
    public TupleDesc getTupleDesc(int tableid) throws NoSuchElementException {
        for (int i = 0; i < tableIds.size(); i++) {
            if (tableIds.get(i) == tableid){
                return dbFiles.get(i).getTupleDesc();
            }
        }
        throw new NoSuchElementException("Catalog.class, func: getTupleDesc, no such table id: " + tableid);
    }

    /**
     * Returns the DbFile that can be used to read the contents of the
     * specified table.
     *
     * @param tableid The id of the table, as specified by the DbFile.getId()
     *                function passed to addTable
     */
    public DbFile getDatabaseFile(int tableid) throws NoSuchElementException {
        for (int i = 0; i < tableIds.size(); i++) {
            if (tableIds.get(i) == tableid){
                return dbFiles.get(i);
            }
        }
        throw new NoSuchElementException("Catalog.class, func: getDatabaseFile, no such table id: " + tableid);
    }

    public String getPrimaryKey(int tableid) {
        for (int i = 0; i < tableIds.size(); i++) {
            if (tableIds.get(i) == tableid){
                return pkeyFields.get(i);
            }
        }
        throw new NoSuchElementException("Catalog.class, func: getPrimaryKey, no such table id: " + tableid);
    }

    public Iterator<Integer> tableIdIterator() {
        return tableIds.iterator();
    }

    public String getTableName(int id) {
        for (int i = 0; i < tableIds.size(); i++) {
            if (tableIds.get(i) == id){
                return names.get(i);
            }
        }
        throw new NoSuchElementException("Catalog.class, func: getTableName, no such table id: " + id);
    }

    /**
     * Delete all tables from the catalog
     */
    public void clear() {
        this.tableIds = new ArrayList<>();
        this.dbFiles = new ArrayList<>();
        this.names = new ArrayList<>();
        this.pkeyFields = new ArrayList<>();
    }

    /**
     * Reads the schema from a file and creates the appropriate tables in the database.
     *
     * @param catalogFile
     */
    public void loadSchema(String catalogFile) {
        String line = "";
        String baseFolder = new File(new File(catalogFile).getAbsolutePath()).getParent();
        try {
            BufferedReader br = new BufferedReader(new FileReader(catalogFile));

            while ((line = br.readLine()) != null) {
                //assume line is of the format name (field type, field type, ...)
                String name = line.substring(0, line.indexOf("(")).trim();
                //System.out.println("TABLE NAME: " + name);
                String fields = line.substring(line.indexOf("(") + 1, line.indexOf(")")).trim();
                String[] els = fields.split(",");
                ArrayList<String> names = new ArrayList<>();
                ArrayList<Type> types = new ArrayList<>();
                String primaryKey = "";
                for (String e : els) {
                    String[] els2 = e.trim().split(" ");
                    names.add(els2[0].trim());
                    if (els2[1].trim().equalsIgnoreCase("int"))
                        types.add(Type.INT_TYPE);
                    else if (els2[1].trim().equalsIgnoreCase("string"))
                        types.add(Type.STRING_TYPE);
                    else {
                        System.out.println("Unknown type " + els2[1]);
                        System.exit(0);
                    }
                    if (els2.length == 3) {
                        if (els2[2].trim().equals("pk"))
                            primaryKey = els2[0].trim();
                        else {
                            System.out.println("Unknown annotation " + els2[2]);
                            System.exit(0);
                        }
                    }
                }
                Type[] typeAr = types.toArray(new Type[0]);
                String[] namesAr = names.toArray(new String[0]);
                TupleDesc t = new TupleDesc(typeAr, namesAr);
                HeapFile tabHf = new HeapFile(new File(baseFolder + "/" + name + ".dat"), t);
                addTable(tabHf, name, primaryKey);
                System.out.println("Added table : " + name + " with schema " + t);
            }
        } catch (IOException e) {
            e.printStackTrace();
            System.exit(0);
        } catch (IndexOutOfBoundsException e) {
            System.out.println("Invalid catalog entry : " + line);
            System.exit(0);
        }
    }
}

