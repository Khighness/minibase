package top.parak.minibase.toolkit;

import java.util.Collection;

/**
 * Requires util.
 *
 * @author Khighness
 * @since 2023-08-02
 */
public final class Requires {

    /**
     * Checks that the specified object reference is not {@code null}.
     *
     * @param obj the object reference to check for nullity
     * @throws IllegalArgumentException if {@code obj} is {@code null}
     */
    public static void requireNotNull(Object obj) {
        if (obj == null) {
            throw new IllegalArgumentException();
        }
    }

    /**
     * Checks that the specified object reference is not {@code null} and
     * throws a customized {@link NullPointerException} if it is.
     *
     * @param obj     the object reference to check for nullity
     * @param message detail message to be used in the event that a {@code
     *                NullPointerException} is thrown
     * @throws IllegalArgumentException if {@code obj} is {@code null}
     */
    public static void requireNotNull(Object obj, String message) {
        if (obj == null) {
            throw new IllegalArgumentException(message);
        }
    }

    /**
     * Ensures the truth of an expression involving one or more parameters
     * to the calling method.
     *
     * @param expression a boolean expression
     * @throws IllegalArgumentException if {@code expression} is false
     */
    public static void requireTrue(boolean expression) {
        if (!expression) {
            throw new IllegalArgumentException();
        }
    }

    /**
     * Ensures the truth of an expression involving one or more parameters
     * to the calling method.
     *
     * @param expression a boolean expression
     * @param message    the exception message to use if the check fails
     *                   {@link String#valueOf(Object)}
     * @throws IllegalArgumentException if {@code expression} is false
     */
    public static void requireTrue(boolean expression, String message) {
        if (!expression) {
            throw new IllegalArgumentException(message);
        }
    }

    /**
     * Ensures the truth of an expression involving one or more parameters
     * to the calling method.
     *
     * @param expression a boolean expression
     * @param fmt        the exception message with format string
     * @param args       arguments referenced by the format specifiers in the format
     *                   string
     * @throws IllegalArgumentException if {@code expression} is false
     */
    public static void requireTrue(boolean expression, String fmt, Object... args) {
        if (!expression) {
            throw new IllegalArgumentException(String.format(fmt, args));
        }
    }

    /**
     * Checks that the specified collection is not {@code null}.
     *
     * @param collection the collection to check for empty
     * @throws IllegalArgumentException if {@code collection} is empty
     */
    public static void requireNotEmpty(Collection<?> collection) {
        if (collection == null || collection.isEmpty()) {
            throw new IllegalArgumentException();
        }
    }

    /**
     * Checks that the specified collection is not {@code null}.
     *
     * @param collection the collection to check for empty
     * @param message    the exception message to use if the check fails
     * @throws IllegalArgumentException if {@code collection} is empty
     */
    public static void requireNotEmpty(Collection<?> collection, String message) {
        if (collection == null || collection.isEmpty()) {
            throw new IllegalArgumentException(message);
        }
    }

    private Requires() {
    }
}
