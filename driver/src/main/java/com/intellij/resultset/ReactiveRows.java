package com.intellij.resultset;

import com.couchbase.client.java.json.JsonObject;
import com.couchbase.client.java.query.ReactiveQueryResult;
import org.jetbrains.annotations.NotNull;

import java.util.Collections;
import java.util.Iterator;
import java.util.stream.Stream;

public class ReactiveRows implements ResultSetRows<JsonObject> {
    private Stream<JsonObject> stream;
    private Iterator<JsonObject> iterator;

    public ReactiveRows(@NotNull ReactiveQueryResult queryResult, int fetchSize) {
        this.stream = queryResult.rowsAsObject()
                .toStream(fetchSize);
        this.iterator = stream.iterator();
    }

    @Override
    public boolean hasNext() {
        return iterator.hasNext();
    }

    @Override
    public JsonObject next() {
        return iterator.next();
    }

    @Override
    public void close() {
        if (stream != null) {
            stream.close();
            iterator = Collections.emptyIterator();
            stream = null;
        }
    }
}
