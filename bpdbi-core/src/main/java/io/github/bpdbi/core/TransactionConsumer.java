package io.github.bpdbi.core;

import org.jspecify.annotations.NonNull;

/**
 * An action executed within a transaction that returns no result. The generic exception type {@code
 * X} allows checked exceptions to propagate without wrapping.
 *
 * @param <X> the exception type that may be thrown
 * @see Connection#useTransaction(TransactionConsumer)
 */
@FunctionalInterface
public interface TransactionConsumer<X extends Exception> {

  /**
   * Execute logic within the given transaction.
   *
   * @param tx the transaction
   * @throws X if an error occurs
   */
  void execute(@NonNull Transaction tx) throws X;
}
