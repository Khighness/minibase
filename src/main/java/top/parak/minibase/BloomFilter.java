package top.parak.minibase;

/**
 * @author KHighness
 * @since 2023-07-29
 */
public class BloomFilter {
    private final int k;
    private final int bitsPerKey;
    private int bitLen ;
    private byte[] result;

    public BloomFilter(int k, int bitsPerKey) {
        if (k < 0) {
            throw new IllegalArgumentException("k can not be negative");
        }
        if (bitsPerKey < 0) {
            throw new IllegalArgumentException("bitsPerKey can not be negative");
        }
        this.k = k;
        this.bitsPerKey = bitsPerKey;
    }

    public byte[] generate(byte[][] keys) {
        if (keys == null) {
            throw new NullPointerException("keys can not be null");
        }

        bitLen = keys.length * bitsPerKey;
        bitLen = Math.max(((bitLen + 7) / 8) * 8 , 64);
        result = new byte[bitLen >> 3];
        for (byte[] key : keys) {
            int h = Bytes.hash(key);
            for (int t = 0; t < k; t++) {
                int idx = (h % bitLen + bitLen) % bitLen;
                result[idx / 8] |= (1 << (idx % 8));
                int delta = (h >> 17) | (h << 15);
                h += delta;
            }
        }
        return result;
    }

    public boolean contains(byte[] keys) {
        if (keys == null) {
            throw new NullPointerException("keys can not be null");
        }

        int h = Bytes.hash(keys);
        for (int t = 0; t < k; t++) {
            int idx = (h % bitLen + bitLen) % bitLen;
            if ((result[idx / 8] & (1 << (idx % 8))) == 0) {
                return false;
            }
            int delta = (h >> 17) | (h << 15);
            h += delta;
        }
        return true;
    }

}
