package simpledb.storage;

import simpledb.common.Type;

import java.io.Serializable;
import java.util.*;

/**
 * TupleDesc describes the schema of a tuple.
 */
public class TupleDesc implements Serializable {

    private List<TDItem> items;

    /**
     * A help class to facilitate organizing the information of each field
     */
    public static class TDItem implements Serializable {

        private static final long serialVersionUID = 1L;

        /**
         * The type of the field
         */
        public final Type fieldType;

        /**
         * The name of the field
         */
        public final String fieldName;

        public TDItem(Type t, String n) {
            this.fieldName = n;
            this.fieldType = t;
        }

        public String toString() {
            return fieldName + "(" + fieldType + ")";
        }
    }

    /**
     * @return An iterator which iterates over all the field TDItems
     * that are included in this TupleDesc
     */
    public Iterator<TDItem> iterator() {
        return items.iterator();
    }

    private static final long serialVersionUID = 1L;

    /**
     * Create a new TupleDesc with typeAr.length fields with fields of the
     * specified types, with associated named fields.
     *
     * @param typeAr  array specifying the number of and types of fields in this
     *                TupleDesc. It must contain at least one entry.
     * @param fieldAr array specifying the names of the fields. Note that names may
     *                be null.
     */
    public TupleDesc(Type[] typeAr, String[] fieldAr) {
        this.items = new ArrayList<>();

        if (typeAr.length == fieldAr.length) {
            for (int i = 0; i < typeAr.length; i++) {
                TDItem item = new TDItem(typeAr[i], fieldAr[i]);
                this.items.add(item);
            }
        }
    }

    /**
     * Constructor. Create a new tuple desc with typeAr.length fields with
     * fields of the specified types, with anonymous (unnamed) fields.
     *
     * @param typeAr array specifying the number of and types of fields in this
     *               TupleDesc. It must contain at least one entry.
     */
    public TupleDesc(Type[] typeAr) {
        String[] fieldAr = new String[typeAr.length];
        for (int i = 0; i < fieldAr.length; i++) {
            fieldAr[i] = "";
        }
        this.items = new ArrayList<>();
        if (typeAr.length == fieldAr.length) {
            for (int i = 0; i < typeAr.length; i++) {
                TDItem item = new TDItem(typeAr[i], fieldAr[i]);
                this.items.add(item);
            }
        }
    }

    /**
     * @return the number of fields in this TupleDesc
     */
    public int numFields() {
        return items.size();
    }

    /**
     * Gets the (possibly null) field name of the ith field of this TupleDesc.
     *
     * @param i index of the field name to return. It must be a valid index.
     * @return the name of the ith field
     * @throws NoSuchElementException if i is not a valid field reference.
     */
    public String getFieldName(int i) throws NoSuchElementException {
        if (i < 0 || i >= items.size()) {
            throw new NoSuchElementException("TupleDesc getFieldName failed, invalid index: " + i);
        }
        return items.get(i).fieldName;
    }

    /**
     * Gets the type of the ith field of this TupleDesc.
     *
     * @param i The index of the field to get the type of. It must be a valid
     *          index.
     * @return the type of the ith field
     * @throws NoSuchElementException if i is not a valid field reference.
     */
    public Type getFieldType(int i) throws NoSuchElementException {
        if (i < 0 || i >= items.size()) {
            throw new NoSuchElementException("TupleDesc getFieldType failed, invalid index: " + i);
        }
        return items.get(i).fieldType;
    }

    /**
     * Find the index of the field with a given name.
     *
     * @param name name of the field.
     * @return the index of the field that is first to have the given name.
     * @throws NoSuchElementException if no field with a matching name is found.
     */
    public int fieldNameToIndex(String name) throws NoSuchElementException {
        for (int i = 0; i < items.size(); i++) {
            TDItem item = items.get(i);
            if (name!=null && name.equals(item.fieldName)){
                return i;
            }
        }
        throw new NoSuchElementException("TupleDesc fieldNameToIndex failed, no such name: " + name);
    }

    /**
     * @return The size (in bytes) of tuples corresponding to this TupleDesc.
     * Note that tuples from a given TupleDesc are of a fixed size.
     */
    public int getSize() {
        // some code goes here
        int bytes = 0;
        TDItem item = null;
        for(int i=0; i<this.items.size(); i++){
            item = this.items.get(i);
            bytes += item.fieldType.getLen();
        }
        return bytes;
    }

    /**
     * Merge two TupleDescs into one, with td1.numFields + td2.numFields fields,
     * with the first td1.numFields coming from td1 and the remaining from td2.
     *
     * @param td1 The TupleDesc with the first fields of the new TupleDesc
     * @param td2 The TupleDesc with the last fields of the TupleDesc
     * @return the new TupleDesc
     */
    public static TupleDesc merge(TupleDesc td1, TupleDesc td2) {
        Type[] types = new Type[td1.numFields() + td2.numFields()];
        String[] names = new String[td1.numFields() + td2.numFields()];
        int i = 0;
        TDItem item = null;

        Iterator<TDItem> it= td1.iterator();
        while(it.hasNext()){
            item = it.next();
            types[i] = item.fieldType;
            names[i] = item.fieldName;
            i++;
        }

        it = td2.iterator();
        while(it.hasNext()){
            item = it.next();
            types[i] = item.fieldType;
            names[i] = item.fieldName;
            i++;
        }

        return new TupleDesc(types, names);
    }

    /**
     * Compares the specified object with this TupleDesc for equality. Two
     * TupleDescs are considered equal if they have the same number of items
     * and if the i-th type in this TupleDesc is equal to the i-th type in o
     * for every i.
     *
     * @param o the Object to be compared for equality with this TupleDesc.
     * @return true if the object is equal to this TupleDesc.
     */

    public boolean equals(Object o) {
        if (!(o instanceof TupleDesc)){
            return false;
        }

        TupleDesc td = (TupleDesc) o;

        Iterator<TDItem> it1 = this.iterator();
        Iterator<TDItem> it2 = td.iterator();

        while(it1.hasNext() && it2.hasNext()){
            TDItem item1 = it1.next();
            TDItem item2 = it2.next();
            if (item1.fieldType.compareTo(item2.fieldType) != 0){
                return false;
            }
        }
        if(it1.hasNext() || it2.hasNext()){
            return false;
        }
        return true;
    }

    public int hashCode() {
        // If you want to use TupleDesc as keys for HashMap, implement this so
        // that equal objects have equals hashCode() results
        throw new UnsupportedOperationException("unimplemented");
    }

    /**
     * Returns a String describing this descriptor. It should be of the form
     * "fieldType[0](fieldName[0]), ..., fieldType[M](fieldName[M])", although
     * the exact format does not matter.
     *
     * @return String describing this descriptor.
     */
    public String toString() {
        StringBuilder sb = new StringBuilder();
        Iterator<TDItem> it = this.iterator();
        TDItem item = null;

        if(it.hasNext()){
            item = it.next();
            sb.append(item.fieldType.toString());
            sb.append("(").append(item.fieldName).append(")");
        }

        while(it.hasNext()){
            item = it.next();
            sb.append(", ").append(item.fieldType.toString());
            sb.append("(").append(item.fieldName).append(")");
        }
        return sb.toString();
    }
}
