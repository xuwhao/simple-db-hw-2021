package simpledb.execution;

import simpledb.common.Type;
import simpledb.storage.*;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.function.BiConsumer;

/**
 * Knows how to compute some aggregate over a set of IntFields.
 */
public class IntegerAggregator implements Aggregator {

    private static final long serialVersionUID = 1L;
    private final Integer gbfield;
    private final Type gbfieldType;
    private final Integer afield;
    private final Map<Field, Integer[]> groupedMap = new HashMap<>();
    private final BiConsumer<Field, IntField> consumer;

    /**
     * Aggregate constructor
     *
     * @param gbfield     the 0-based index of the group-by field in the tuple, or
     *                    NO_GROUPING if there is no grouping
     * @param gbfieldtype the type of the group by field (e.g., Type.INT_TYPE), or null
     *                    if there is no grouping
     * @param afield      the 0-based index of the aggregate field in the tuple
     * @param what        the aggregation operator
     */

    public IntegerAggregator(int gbfield, Type gbfieldtype, int afield, Op what) {
        // some code goes here
        this.gbfield = gbfield;
        this.gbfieldType = gbfieldtype;
        this.afield = afield;
        switch (what) {
            case MIN:
                consumer = (field, min) -> {
                    if (groupedMap.containsKey(field)) {
                        Integer[] v = groupedMap.get(field);
                        v[0] = v[0] <= min.getValue() ? v[0] : min.getValue();
                    } else {
                        groupedMap.put(field, new Integer[]{min.getValue()});
                    }
                };
                break;
            case AVG:
                consumer = (field, val) -> {
                    if (groupedMap.containsKey(field)) {
                        Integer[] v = groupedMap.get(field);
                        v[1] += val.getValue();
                        v[2]++;
                        v[0] = v[1] / v[2];
                    } else {
                        groupedMap.put(field, new Integer[]{val.getValue(), val.getValue(), 1});
                    }
                };
                break;
            case SUM:
                consumer = (field, val) -> {
                    if (groupedMap.containsKey(field)) {
                        groupedMap.get(field)[0] += val.getValue();
                    } else {
                        groupedMap.put(field, new Integer[]{val.getValue()});
                    }
                };
                break;
            case COUNT:
                consumer = (field, val) -> {
                    if (groupedMap.containsKey(field)) {
                        groupedMap.get(field)[0] += 1;
                    } else {
                        groupedMap.put(field, new Integer[]{1});
                    }
                };
                break;
            case MAX:
                consumer = (field, max) -> {
                    if (groupedMap.containsKey(field)) {
                        Integer[] v = groupedMap.get(field);
                        v[0] = v[0] >= max.getValue() ? v[0] : max.getValue();
                    } else {
                        groupedMap.put(field, new Integer[]{max.getValue()});
                    }
                };
                break;
            default:
                throw new UnsupportedOperationException("unsupported op: " + what);
        }
    }

    /**
     * Merge a new tuple into the aggregate, grouping as indicated in the
     * constructor
     *
     * @param tup the Tuple containing an aggregate field and a group-by field
     */
    public void mergeTupleIntoGroup(Tuple tup) {
        // some code goes here
        Field f = null;
        if (gbfield != NO_GROUPING) {
            f = tup.getField(gbfield);
        }
        consumer.accept(f, (IntField) tup.getField(afield));
    }

    /**
     * Create a OpIterator over group aggregate results.
     *
     * @return a OpIterator whose tuples are the pair (groupVal, aggregateVal)
     * if using group, or a single (aggregateVal) if no grouping. The
     * aggregateVal is determined by the type of aggregate specified in
     * the constructor.
     */
    public OpIterator iterator() {
        // some code goes here
        List<Tuple> tuples = new ArrayList<>();
        final BiConsumer<Field, Integer[]> biConsumer;
        final TupleDesc tupleDesc;

        if (gbfield == NO_GROUPING){
            tupleDesc = new TupleDesc(new Type[]{Type.INT_TYPE}, new String[]{"AggregateValue"});
            biConsumer = (k, v)->{
                Tuple tuple = new Tuple(tupleDesc);
                tuple.setField(0, new IntField(v[0]));
                tuples.add(tuple);
            };
        }else{
            tupleDesc = new TupleDesc(new Type[]{gbfieldType, Type.INT_TYPE}, new String[]{"GroupValue", "AggregateValue"});
            biConsumer = (k, v)->{
                Tuple tuple = new Tuple(tupleDesc);
                tuple.setField(0, k);
                tuple.setField(1, new IntField(v[0]));
                tuples.add(tuple);
            };
        }

        groupedMap.forEach(biConsumer::accept);
        return new TupleIterator(tupleDesc, tuples);
    }

}
