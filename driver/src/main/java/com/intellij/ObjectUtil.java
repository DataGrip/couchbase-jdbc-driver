package com.intellij;

import org.jetbrains.annotations.Contract;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

public final class ObjectUtil {
    private ObjectUtil() {}

    @Contract(value = "null, _ -> null", pure = true)
    public static @Nullable <T> T tryCast(@Nullable Object obj, @NotNull Class<T> clazz) {
        if (clazz.isInstance(obj)) {
            return clazz.cast(obj);
        }
        return null;
    }
}
