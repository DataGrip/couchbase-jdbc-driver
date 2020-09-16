package com.intellij;

import org.jetbrains.annotations.Contract;
import org.jetbrains.annotations.NotNull;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.function.Function;

public final class ContainerUtil {
    private ContainerUtil() {
    }

    @Contract(pure = true)
    public static @NotNull <T, V> List<V> map(@NotNull Collection<? extends T> collection, @NotNull Function<? super T, ? extends V> mapping) {
        if (collection.isEmpty()) return new ArrayList<>();
        List<V> list = new ArrayList<>(collection.size());
        for (final T t : collection) {
            list.add(mapping.apply(t));
        }
        return list;
    }
}
