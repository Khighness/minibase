package top.parak.minibase.toolkit;

/**
 * Bloom filter.
 *
 * @author KHighness
 * @since 2023-07-29
 */
public class BloomFilter {

    /**
     * The number of hash function.
     */
    private final int k;

    /**
     * The bit count per key.
     */
    private final int bitsPerKey;

    /**
     * The total bit length.
     */
    private int bitLen;

    /**
     * The array to store bit value.
     */
    private byte[] result;

    /**
     * Create a BloomFilter instance.
     *
     * @param k          the number of hash function
     * @param bitsPerKey the bit count per key
     */
    public BloomFilter(int k, int bitsPerKey) {
        Requires.requireTrue(k > 0, "k must be positive");
        Requires.requireTrue(bitsPerKey > 0, "bitsPerKey must be positive");

        this.k = k;
        this.bitsPerKey = bitsPerKey;
    }

    /**
     * Generate the bloom filter corresponding to the given keys.
     *
     * @param keys the keys
     * @return the result
     */
    public byte[] generate(byte[][] keys) {
        Requires.requireNotNull(keys);

        bitLen = keys.length * bitsPerKey;
        bitLen = Math.max(((bitLen + 7) / 8) * 8 , 64);
        result = new byte[bitLen >> 3];
        for (byte[] key : keys) {
            int h = Bytes.hash(key);
            for (int t = 0; t < k; t++) {
                int idx = (h % bitLen + bitLen) % bitLen;
                result[idx >> 3] |= (1 << (idx % 8));
                int delta = (h >> 17) | (h << 15);
                h += delta;
            }
        }
        return result;
    }

    /**
     * Check if the bloom filter contains the specified key.
     *
     * @param key the byte array of the key to check
     * @return true if bloom filter maybe contains the specified key,
     *         false if bloom filter must not contain the specified key.
     */
    public boolean contains(byte[] key) {
        Requires.requireNotNull(key);

        int h = Bytes.hash(key);
        for (int t = 0; t < k; t++) {
            int idx = (h % bitLen + bitLen) % bitLen;
            if ((result[idx >> 3] & (1 << (idx % 8))) == 0) {
                return false;
            }
            int delta = (h >> 17) | (h << 15);
            h += delta;
        }
        return true;
    }

}
