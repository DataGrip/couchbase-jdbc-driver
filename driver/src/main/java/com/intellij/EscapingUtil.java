package com.intellij;

import org.jetbrains.annotations.Contract;
import org.jetbrains.annotations.NotNull;
import reactor.util.annotation.Nullable;

public class EscapingUtil {
    private static final String BACKQUOTE = "`";

    private EscapingUtil() {
        //empty
    }

    @Nullable
    @Contract("null -> null")
    public static String stripBackquotes(@Nullable String s) {
        if (s == null) {
            return null;
        }
        if (s.startsWith(BACKQUOTE) && s.endsWith(BACKQUOTE)) {
            return s.substring(1, s.length() - 1);
        }
        return s;
    }

    @Nullable
    @Contract("null -> null")
    public static String wrapInBackquotes(@Nullable String s) {
        if (s == null) {
            return null;
        }
        return BACKQUOTE + s + BACKQUOTE;
    }

    @NotNull
    public static String escapeChars(@NotNull final String str, final char... character) {
        final StringBuilder buf = new StringBuilder(str);
        for (char c : character) {
            escapeChar(buf, c);
        }
        return buf.toString();
    }

    public static void escapeChar(@NotNull final StringBuilder buf, final char character) {
        int idx = 0;
        while ((idx = indexOf(buf, character, idx)) >= 0) {
            buf.insert(idx, "\\");
            idx += 2;
        }
    }

    @Contract(pure = true)
    public static int indexOf(@NotNull CharSequence s, char c, int start) {
        return indexOf(s, c, start, s.length());
    }

    @Contract(pure = true)
    public static int indexOf(@NotNull CharSequence s, char c, int start, int end) {
        end = Math.min(end, s.length());
        for (int i = Math.max(start, 0); i < end; i++) {
            if (s.charAt(i) == c) return i;
        }
        return -1;
    }
}
