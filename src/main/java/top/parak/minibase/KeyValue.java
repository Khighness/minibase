package top.parak.minibase;

/**
 * Key-Value.
 *
 * @author cantai
 * @since 2023-08-02
 */
public class KeyValue implements Comparable<KeyValue> {

    public static final int RAW_KEY_LEN_SIZE = 4;
    public static final int VAL_LEN_SIZE = 4;
    public static final int OP_SIZE = 1;
    public static final int SEQ_ID_SIZE = 8;

    private byte[] key;
    private byte[] value;
    private Op     op;
    private long   sequenceId;

    @Override
    public int compareTo(KeyValue o) {
        return 0;
    }

    public KeyValue(byte[] key, byte[] value, Op op, long sequenceId) {
        Requires.requireNotNull(key);
        Requires.requireNotNull(value);
        Requires.requireNotNull(op);
        Requires.requireTrue(sequenceId > 0);

        this.key = key;
        this.value = value;
        this.op = op;
        this.sequenceId = sequenceId;
    }

    public static KeyValue create(byte[] key, byte[] value, Op op, long sequenceId) {
        return new KeyValue(key, value, op, sequenceId);
    }

    public static KeyValue createPut(byte[] key, byte[] value, Op op, long sequenceId) {
        return new KeyValue(key, value, Op.Put, sequenceId);
    }

    public static KeyValue createDelete(byte[] key, byte[] value, long sequenceId) {
        return new KeyValue(key, value, Op.Delete, sequenceId);
    }

}
