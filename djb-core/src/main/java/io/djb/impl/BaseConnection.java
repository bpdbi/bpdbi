package io.djb.impl;

import io.djb.BinaryCodec;
import io.djb.Connection;
import io.djb.ColumnDescriptor;
import io.djb.JsonMapper;
import io.djb.MapperRegistry;
import io.djb.Row;
import io.djb.RowSet;
import io.djb.TypeRegistry;

import io.djb.ConnectionConfig;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import org.jspecify.annotations.Nullable;

/**
 * Abstract base class that implements the shared pipeline logic (enqueue/flush/query).
 * Database-specific drivers extend this and implement the protocol-level methods.
 */
public abstract class BaseConnection implements Connection {

    private final List<PendingStatement> pending = new ArrayList<>();
    private TypeRegistry typeRegistry = TypeRegistry.defaults();
    private MapperRegistry mapperRegistry = MapperRegistry.defaults();
    private @Nullable JsonMapper jsonMapper;
    protected @Nullable PreparedStatementCache psCache;
    protected int cacheSqlLimit;

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

    public void setJsonMapper(JsonMapper mapper) {
        this.jsonMapper = mapper;
    }

    public @Nullable JsonMapper jsonMapper() {
        return jsonMapper;
    }

    /**
     * Initialize the prepared statement cache from config. Called by subclass constructors.
     */
    protected void initCache(ConnectionConfig config) {
        if (config != null && config.cachePreparedStatements()) {
            this.psCache = new PreparedStatementCache(config.preparedStatementCacheMaxSize());
            this.cacheSqlLimit = config.preparedStatementCacheSqlLimit();
        }
    }

    /**
     * Check if a SQL string is eligible for caching.
     */
    protected boolean isCacheable(String sql) {
        return psCache != null && sql.length() <= cacheSqlLimit;
    }

    /**
     * Handle evicted statements by closing them server-side. Subclasses implement the actual close.
     */
    protected void handleEvicted(List<PreparedStatementCache.CachedStatement> evicted) {
        for (var stmt : evicted) {
            closeCachedStatement(stmt);
        }
    }

    /**
     * Close a cached statement server-side. DB-specific implementations override this.
     */
    protected abstract void closeCachedStatement(PreparedStatementCache.CachedStatement stmt);

    @Override
    public RowSet query(String sql) {
        enqueue(sql);
        var result = flush().getLast();
        if (result.hasError()) throw result.getError();
        return result;
    }

    @Override
    public RowSet query(String sql, @Nullable Object... params) {
        enqueue(sql, params);
        var result = flush().getLast();
        if (result.hasError()) throw result.getError();
        return result;
    }

    @Override
    public RowSet query(String sql, Map<String, @Nullable Object> params) {
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
    public int enqueue(String sql, @Nullable Object... params) {
        int index = pending.size();
        String[] textParams = new String[params.length];
        for (int i = 0; i < params.length; i++) {
            if (params[i] != null && jsonMapper != null && typeRegistry.isJsonType(params[i].getClass())) {
                textParams[i] = jsonMapper.toJson(params[i]);
            } else {
                textParams[i] = typeRegistry.bind(params[i]);
            }
        }
        pending.add(new PendingStatement(sql, textParams, false));
        return index;
    }

    @Override
    public int enqueue(String sql, Map<String, @Nullable Object> params) {
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

        // Phase 1: Encode simple queries into the buffer (pipelineable)
        //          Extended queries may need request-response (MySQL prepare+execute)
        List<RowSet> results = new ArrayList<>(toFlush.size());

        // Collect consecutive simple queries for batched send
        int i = 0;
        while (i < toFlush.size()) {
            // Batch consecutive simple queries
            int batchStart = i;
            while (i < toFlush.size() && toFlush.get(i).simple) {
                encodeSimpleQuery(toFlush.get(i).sql);
                i++;
            }

            if (i > batchStart) {
                // Flush the batch of simple queries
                flushToNetwork();
                for (int j = batchStart; j < i; j++) {
                    results.add(readSimpleQueryResponse());
                }
            }

            // Handle extended query (may need synchronous prepare+execute)
            if (i < toFlush.size() && !toFlush.get(i).simple) {
                PendingStatement stmt = toFlush.get(i);
                results.add(executeExtendedQuery(stmt.sql, stmt.paramValues));
                i++;
            }
        }

        return results;
    }

    /**
     * Create a Row with the connection's binary codec and mapper registry.
     * Subclasses should use this instead of calling the Row constructor directly.
     */
    protected Row createRow(ColumnDescriptor[] columns, byte @Nullable [][] values) {
        return new Row(columns, values, binaryCodec(), mapperRegistry, jsonMapper, typeRegistry.jsonTypes());
    }

    /**
     * Return the binary codec for this database driver, or null if not applicable.
     * Subclasses override to provide their driver-specific codec.
     */
    protected @Nullable BinaryCodec binaryCodec() {
        return null;
    }

    @Override
    public void close() {
        if (psCache != null) {
            for (var stmt : psCache.values()) {
                closeCachedStatement(stmt);
            }
            psCache.clear();
        }
        sendTerminate();
        closeTransport();
    }

    // --- Abstract methods for database-specific protocol ---

    /** Encode a simple (non-parameterized) query into the write buffer. */
    protected abstract void encodeSimpleQuery(String sql);

    /** Flush the write buffer to the network. */
    protected abstract void flushToNetwork();

    /** Read the response for a simple query. */
    protected abstract RowSet readSimpleQueryResponse();

    /**
     * Execute a parameterized query. For PG this can pipeline (encode+flush+read).
     * For MySQL this does prepare+execute synchronously (binary protocol).
     */
    protected abstract RowSet executeExtendedQuery(String sql, String @Nullable [] params);

    protected abstract void sendTerminate();
    protected abstract void closeTransport();

    /**
     * Return the placeholder prefix for this database.
     * PG uses "$" (for $1, $2, ...), MySQL uses "?" (positional).
     */
    protected abstract String placeholderPrefix();

    // --- Internal ---

    protected record PendingStatement(String sql, String @Nullable [] paramValues, boolean simple) {}
}
