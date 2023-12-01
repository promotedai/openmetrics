/*
 * Forked from
 * package com.github.benmanes.caffeine.examples.coalescing.bulkloader;
 *
 * Stripped out unused code.
 *
 * Copyright 2019 Guus C. Bloemsma. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package ai.promoted.metrics.logprocessor.common.functions.content.datastream;

import com.github.benmanes.caffeine.cache.AsyncCacheLoader;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executor;

/**
 * An implementation of {@link AsyncCacheLoader} that delegates everything to another bulk loader.
 */
public class CoalescingBulkCacheLoader<Key, Value> implements AsyncCacheLoader<Key, Value> {
  private final BulkLoader<Key, Value> bulkLoader;

  public static <Key, Value> CoalescingBulkCacheLoader<Key, Value> create(
      BulkLoader<Key, Value> bulkLoader) {
    return new CoalescingBulkCacheLoader<>(bulkLoader);
  }

  private CoalescingBulkCacheLoader(BulkLoader<Key, Value> executor) {
    this.bulkLoader = executor;
  }

  @Override
  public CompletableFuture<Value> asyncLoad(Key key, Executor executor) {
    return bulkLoader.add(key);
  }
}
