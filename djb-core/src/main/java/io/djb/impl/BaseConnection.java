package io.djb.impl;

import io.djb.*;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/**
 * Abstract base class that implements the shared pipeline logic (enqueue/flush/query).
 * Database-specific drivers extend this and implement the protocol-level methods.
 */
public abstract class BaseConnection implements Connection {

    private final List<PendingStatement> pending = new ArrayList<>();
    private TypeRegistry typeRegistry = TypeRegistry.defaults();
    private MapperRegistry mapperRegistry = MapperRegistry.defaults();

    public void setTypeRegistry(TypeRegistry registry) {
        this.typeRegistry = registry;
    }

    public TypeRegistry typeRegistry() {
        return typeRegistry;
    }

    public void setMapperRegistry(MapperRegistry registry) {
        this.mapperRegistry = registry;
    }

    public MapperRegistry mapperRegistry() {
        return mapperRegistry;
    }

    @Override
    public RowSet query(String sql) {
        enqueue(sql);
        var result = flush().getLast();
        if (result.hasError()) throw result.getError();
        return result;
    }

    @Override
    public RowSet query(String sql, Object... params) {
        enqueue(sql, params);
        var result = flush().getLast();
        if (result.hasError()) throw result.getError();
        return result;
    }

    @Override
    public RowSet query(String sql, Map<String, Object> params) {
        var parsed = NamedParamParser.parse(sql, params, placeholderPrefix(), typeRegistry);
        enqueue(parsed.sql(), (Object[]) parsed.params());
        var result = flush().getLast();
        if (result.hasError()) throw result.getError();
        return result;
    }

    @Override
    public int enqueue(String sql) {
        int index = pending.size();
        pending.add(new PendingStatement(sql, null, true));
        return index;
    }

    @Override
    public int enqueue(String sql, Object... params) {
        int index = pending.size();
        String[] textParams = new String[params.length];
        for (int i = 0; i < params.length; i++) {
            textParams[i] = typeRegistry.bind(params[i]);
        }
        pending.add(new PendingStatement(sql, textParams, false));
        return index;
    }

    @Override
    public int enqueue(String sql, Map<String, Object> params) {
        var parsed = NamedParamParser.parse(sql, params, placeholderPrefix(), typeRegistry);
        return enqueue(parsed.sql(), (Object[]) parsed.params());
    }

    @Override
    public List<RowSet> flush() {
        if (pending.isEmpty()) {
            return List.of();
        }

        List<PendingStatement> toFlush = new ArrayList<>(pending);
        pending.clear();

        // Phase 1: Encode all statements into one buffer
        for (PendingStatement stmt : toFlush) {
            if (stmt.simple) {
                encodeSimpleQuery(stmt.sql);
            } else {
                encodeExtendedQuery(stmt.sql, stmt.paramValues);
            }
        }

        // Phase 2: Single network write
        flushToNetwork();

        // Phase 3: Read responses for each statement, propagate mapper registry to rows
        List<RowSet> results = new ArrayList<>(toFlush.size());
        for (PendingStatement stmt : toFlush) {
            RowSet rs = stmt.simple ? readSimpleQueryResponse() : readExtendedQueryResponse();
            propagateMapperRegistry(rs);
            results.add(rs);
        }
        return results;
    }

    private void propagateMapperRegistry(RowSet rs) {
        if (mapperRegistry != null && !rs.hasError()) {
            for (Row row : rs) {
                row.setMapperRegistry(mapperRegistry);
            }
        }
    }

    @Override
    public void close() {
        sendTerminate();
        closeTransport();
    }

    // --- Abstract methods for database-specific protocol ---

    protected abstract void encodeSimpleQuery(String sql);
    protected abstract void encodeExtendedQuery(String sql, String[] params);
    protected abstract void flushToNetwork();
    protected abstract RowSet readSimpleQueryResponse();
    protected abstract RowSet readExtendedQueryResponse();
    protected abstract void sendTerminate();
    protected abstract void closeTransport();

    /**
     * Return the placeholder prefix for this database.
     * PG uses "$" (for $1, $2, ...), MySQL uses "?" (positional).
     */
    protected abstract String placeholderPrefix();

    // --- Internal ---

    protected record PendingStatement(String sql, String[] paramValues, boolean simple) {}
}
