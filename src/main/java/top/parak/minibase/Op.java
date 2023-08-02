package top.parak.minibase;

/**
 * Op type.
 *
 * @author cantai
 * @since 2023-08-02
 */
public enum Op {
    Put((byte) 0),
    Delete((byte) 1),
    ;

    private final byte code;

    private Op(byte code) {
        this.code = code;
    }

    public byte getCode() {
        return code;
    }

    public static Op getByCode(byte code) {
        switch (code) {
            case 0:
                return Put;
            case 1:
                return Delete;
            default:
                throw new IllegalArgumentException("Unknown code: " + code);
        }
    }
}
