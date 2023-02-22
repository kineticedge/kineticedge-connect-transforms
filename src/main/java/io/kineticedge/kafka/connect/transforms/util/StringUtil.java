package io.kineticedge.kafka.connect.transforms.util;

/**
 * There is no dependency on connect runtime for a string utils library, so brining in
 * a minimal set of string utilities directly into this project.
 */
public final class StringUtil {

    private StringUtil() {
    }

    public static int length(final CharSequence cs) {
        return cs == null ? 0 : cs.length();
    }

    public static boolean isBlank(final CharSequence cs) {
        final int strLen = length(cs);
        if (strLen == 0) {
            return true;
        }
        for (int i = 0; i < strLen; i++) {
            if (!Character.isWhitespace(cs.charAt(i))) {
                return false;
            }
        }
        return true;
    }

    public static boolean isNotBlank(final CharSequence cs) {
        return !isBlank(cs);
    }

}
