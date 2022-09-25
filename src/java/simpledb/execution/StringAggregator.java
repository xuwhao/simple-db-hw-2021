package simpledb.execution;

import simpledb.common.Type;
import simpledb.storage.*;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.function.BiConsumer;

/**
 * Knows how to compute some aggregate over a set of StringFields.
 */
public class StringAggregator implements Aggregator {

    private static final long serialVersionUID = 1L;
    private final int gbfield;
    private final Type gbfieldtype;
    private final int afield;
    private final Map<Field, Integer> groupedMap = new HashMap<>();
    private final BiConsumer<Field, StringField> consumer;

    /**
     * Aggregate constructor
     * @param gbfield the 0-based index of the group-by field in the tuple, or NO_GROUPING if there is no grouping
     * @param gbfieldtype the type of the group by field (e.g., Type.INT_TYPE), or null if there is no grouping
     * @param afield the 0-based index of the aggregate field in the tuple
     * @param what aggregation operator to use -- only supports COUNT
     * @throws IllegalArgumentException if what != COUNT
     */

    public StringAggregator(int gbfield, Type gbfieldtype, int afield, Op what) {
        // some code goes here
        this.gbfield = gbfield;
        this.gbfieldtype = gbfieldtype;
        this.afield = afield;
        switch (what){
            case COUNT:
                consumer = (field, val) -> {
                    if (groupedMap.containsKey(field)) {
                        groupedMap.put(field, groupedMap.get(field) + 1);
                    } else {
                        groupedMap.put(field, 1);
                    }
                };
                break;
            default:
                throw new UnsupportedOperationException("unsupported op: " + what);
        }
    }

    /**
     * Merge a new tuple into the aggregate, grouping as indicated in the constructor
     * @param tup the Tuple containing an aggregate field and a group-by field
     */
    public void mergeTupleIntoGroup(Tuple tup) {
        // some code goes here
        Field f = null;
        if (gbfield != NO_GROUPING) {
            f = tup.getField(gbfield);
        }
        consumer.accept(f, (StringField) tup.getField(afield));
    }

    /**
     * Create a OpIterator over group aggregate results.
     *
     * @return a OpIterator whose tuples are the pair (groupVal,
     *   aggregateVal) if using group, or a single (aggregateVal) if no
     *   grouping. The aggregateVal is determined by the type of
     *   aggregate specified in the constructor.
     */
    public OpIterator iterator() {
        // some code goes here
        List<Tuple> tuples = new ArrayList<>();
        final BiConsumer<Field, Integer> biConsumer;
        final TupleDesc tupleDesc;

        if (gbfield == NO_GROUPING){
            tupleDesc = new TupleDesc(new Type[]{Type.INT_TYPE}, new String[]{"AggregateValue"});
            biConsumer = (k, v)->{
                Tuple tuple = new Tuple(tupleDesc);
                tuple.setField(0, new IntField(v));
                tuples.add(tuple);
            };
        }else{
            tupleDesc = new TupleDesc(new Type[]{gbfieldtype, Type.INT_TYPE}, new String[]{"GroupValue", "AggregateValue"});
            biConsumer = (k, v)->{
                Tuple tuple = new Tuple(tupleDesc);
                tuple.setField(0, k);
                tuple.setField(1, new IntField(v));
                tuples.add(tuple);
            };
        }

        groupedMap.forEach(biConsumer::accept);
        return new TupleIterator(tupleDesc, tuples);
    }

}
