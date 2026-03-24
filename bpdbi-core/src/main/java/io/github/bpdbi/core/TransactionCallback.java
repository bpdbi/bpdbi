package io.github.bpdbi.core;

import org.jspecify.annotations.NonNull;

/**
 * A function executed within a transaction that returns a result. The generic exception type {@code
 * X} allows checked exceptions to propagate without wrapping.
 *
 * @param <T> the return type
 * @param <X> the exception type that may be thrown
 * @see Connection#inTransaction(TransactionCallback)
 */
@FunctionalInterface
public interface TransactionCallback<T, X extends Exception> {

  /**
   * Execute logic within the given transaction.
   *
   * @param tx the transaction
   * @return the result
   * @throws X if an error occurs
   */
  T execute(@NonNull Transaction tx) throws X;
}
