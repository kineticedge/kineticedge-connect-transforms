package io.kineticedge.kafka.connect.transforms.util;

import java.util.Collection;

public final class CollectionUtil {

    private CollectionUtil() {
    }

    public static boolean isEmpty(final Collection<?> collection) {
        return collection == null || collection.isEmpty();
    }
}
