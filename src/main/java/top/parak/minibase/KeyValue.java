package top.parak.minibase;

import top.parak.minibase.toolkit.Bytes;
import top.parak.minibase.toolkit.Requires;

import java.io.IOException;

/**
 * Key-Value.
 *
 * <p>Encode bytes structure</p>
 * <pre>
 *     +-------------+-------------+---------+----+---------+---------+
 *     | raw key len |   val len   |   key   | op |  seq id |  value  |
 *     +-------------+-------------+---------+----+---------+---------+
 *     |      4      |      4      | key len | 1  |    8    | val len |
 *     +-------------+-------------+---------+----+---------+---------+
 * </pre>
 *
 * @author Khighness
 * @since 2023-08-02
 */
public class KeyValue implements Comparable<KeyValue> {

    public static final int RAW_KEY_LEN_SIZE = 4;
    public static final int VAL_LEN_SIZE = 4;
    public static final int OP_SIZE = 1;
    public static final int SEQ_ID_SIZE = 8;

    private final byte[] key;
    private final byte[] value;
    private final Op     op;
    private final long   sequenceId;

    private KeyValue(byte[] key, byte[] value, Op op, long sequenceId) {
        Requires.requireNotNull(key);
        Requires.requireNotNull(value);
        Requires.requireNotNull(op);
        Requires.requireTrue(sequenceId > 0);

        this.key = key;
        this.value = value;
        this.op = op;
        this.sequenceId = sequenceId;
    }

    public byte[] getKey() {
        return key;
    }

    public byte[] getValue() {
        return value;
    }

    public Op getOp() {
        return op;
    }

    public long getSequenceId() {
        return sequenceId;
    }

    public int getRawKeyLenSize() {
        return key.length + OP_SIZE + SEQ_ID_SIZE;
    }

    /**
     * Gte the length of the serialized byte array.
     *
     * @return the size of the serialized byte array
     */
    public int getSerializeSize() {
        return RAW_KEY_LEN_SIZE + VAL_LEN_SIZE + getRawKeyLenSize() + value.length;
    }

    /**
     * Serialize the KeyValue into a byte array.
     *
     * @return the serialized byte array
     */
    public byte[] serialize() {
        int rawKeyLen = getRawKeyLenSize();
        int pos = 0;
        byte[] bytes = new byte[getSerializeSize()];

        // Encode raw key length
        byte[] rawKeyLenBytes = Bytes.toBytes(rawKeyLen);
        System.arraycopy(rawKeyLenBytes, 0, bytes, pos, RAW_KEY_LEN_SIZE);
        pos += RAW_KEY_LEN_SIZE;

        // Encode value length
        byte[] valLenBytes = Bytes.toBytes(value.length);
        System.arraycopy(valLenBytes, 0, bytes, pos, VAL_LEN_SIZE);
        pos += VAL_LEN_SIZE;

        // Encode key
        System.arraycopy(key, 0, bytes, pos, key.length);
        pos += key.length;

        // Encode op
        bytes[pos] = op.getCode();
        pos++;

        // Encode sequenceId
        byte[] seqIdBytes = Bytes.toBytes(sequenceId);
        System.arraycopy(seqIdBytes, 0, bytes, pos, seqIdBytes.length);
        pos += seqIdBytes.length;

        // Encode value
        System.arraycopy(value, 0, bytes, pos, value.length);

        return bytes;
    }

    /**
     * Compares this KeyValue with the specified KeyValue for order.
     *
     * <p>Sort rule:</p>
     * <ul>
     *     <li>The smaller the key, the higher the sorting.</li>
     *     <li>The bigger the sequenceId, the higher the sorting.</li>
     *     <li>Delete operations are sorted higher than put operations.</li>
     * </ul>
     *
     * @param that the KeyValue to be compared.
     * @return a negative integer, zero, or a positive integer as this KeyValue
     *         is less than, equal to, or greater than the specified KeyValue.
     */
    @Override
    public int compareTo(KeyValue that) {
        Requires.requireNotNull(key);

        int ret = Bytes.compare(this.key, that.key);
        if (ret != 0) {
            return ret;
        }
        if (this.sequenceId != that.sequenceId) {
            return this.sequenceId > that.sequenceId ? -1 : 1;
        }
        if (this.op != that.op) {
            return this.op.getCode() > that.op.getCode() ? -1 : 1;
        }
        return 0;
    }

    @Override
    public boolean equals(Object obj) {
        if (obj == null) return false;
        if (! (obj instanceof KeyValue)) return false;
        KeyValue that = (KeyValue) obj;
        return this.compareTo(that) == 0;
    }

    /**
     * Create a KeyValue instance.
     *
     * @param key   the byte array of key
     * @param value the byte array of value
     * @param op    the op type
     * @param sequenceId the sequence id
     * @return a KeyValue instance.
     */
    public static KeyValue create(byte[] key, byte[] value, Op op, long sequenceId) {
        return new KeyValue(key, value, op, sequenceId);
    }

    /**
     * Create a KeyValue instance whose op is {@link Op#Put}.
     *
     * @param key        the byte array of key
     * @param value      the byte array of value
     * @param sequenceId the sequence id
     * @return a KeyValue instance.
     */
    public static KeyValue createPut(byte[] key, byte[] value, Op op, long sequenceId) {
        return new KeyValue(key, value, Op.Put, sequenceId);
    }

    /**
     * Create a KeyValue instance whose op is {@link Op#Delete}.
     *
     * @param key        the byte array of key
     * @param value      the byte array of value
     * @param sequenceId the sequence id
     * @return a KeyValue instance.
     */
    public static KeyValue createDelete(byte[] key, byte[] value, long sequenceId) {
        return new KeyValue(key, value, Op.Delete, sequenceId);
    }

    /**
     * Deserialize a KeyValue instance from the specified byte array.
     *
     * @param bytes the specified byte array to be deserialized
     * @return a KeyValue instance
     * @throws IOException if the specified byte array is invalid
     */
    public static KeyValue deserialize(byte[] bytes) throws IOException {
        return deserializeFrom(bytes, 0);
    }

    /**
     * Deserialize a KeyValue instance from the specified offset into the specified byte array.
     *
     * @param bytes  the specified byte array to be deserialized
     * @param offset the specified offset of the array byte
     * @return a KeyValue instance
     * @throws IOException if the specified byte array is invalid
     */
    public static KeyValue deserializeFrom(byte[] bytes, int offset) throws IOException {
        Requires.requireNotNull(bytes, "bytes if null");

        // Decode raw key length
        int pos = offset;
        int rawKeyLen = Bytes.toInt(Bytes.slice(bytes, pos, RAW_KEY_LEN_SIZE));
        pos += RAW_KEY_LEN_SIZE;

        // Decode value length
        int valLen = Bytes.toInt(Bytes.slice(bytes, pos, VAL_LEN_SIZE));
        pos += VAL_LEN_SIZE;

        // Decode key
        int keyLen = rawKeyLen - OP_SIZE - SEQ_ID_SIZE;
        byte[] key = Bytes.slice(bytes, pos, keyLen);
        pos += keyLen;

        // Decode op
        Op op = Op.getByCode(bytes[pos]);
        pos++;

        // Decode sequenceId
        long sequenceId = Bytes.toLong(Bytes.slice(bytes, pos, SEQ_ID_SIZE));
        pos += SEQ_ID_SIZE;

        // Decode value
        byte[] val = Bytes.slice(bytes, pos, valLen);
        return create(key, val, op, sequenceId);
    }

}
