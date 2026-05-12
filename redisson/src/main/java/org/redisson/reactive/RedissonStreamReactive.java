/**
 * Copyright (c) 2013-2026 Nikita Koksharov
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.redisson.reactive;

import org.redisson.api.RStream;
import org.redisson.api.stream.*;
import reactor.core.publisher.Mono;

import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;

/**
 *
 * @author Nikita Koksharov
 *
 * @param <K> key type
 * @param <V> value type
 */
public class RedissonStreamReactive<K, V> {

    private final CommandReactiveExecutor commandExecutor;
    private final RStream<K, V> instance;

    public RedissonStreamReactive(CommandReactiveExecutor commandExecutor, RStream<K, V> instance) {
        this.commandExecutor = commandExecutor;
        this.instance = instance;
    }

    public Mono<Map<StreamMessageId, Map<K, V>>> read(StreamReadArgs args) {
        return commandExecutor.reactive(() -> instance.readAsync(args))
                .filter(m -> !m.isEmpty());
    }

    public Mono<Map<String, Map<StreamMessageId, Map<K, V>>>> read(StreamMultiReadArgs args) {
        return commandExecutor.reactive(() -> instance.readAsync(args))
                .filter(m -> !m.isEmpty());
    }

    public Mono<Map<StreamMessageId, Map<K, V>>> readGroup(String groupName, String consumerName, StreamReadGroupArgs args) {
        return commandExecutor.reactive(() -> instance.readGroupAsync(groupName, consumerName, args))
                .filter(m -> !m.isEmpty());
    }

    public Mono<Map<String, Map<StreamMessageId, Map<K, V>>>> readGroup(String groupName, String consumerName, StreamMultiReadGroupArgs args) {
        return commandExecutor.reactive(() -> instance.readGroupAsync(groupName, consumerName, args))
                .filter(m -> !m.isEmpty());
    }

    public Mono<List<PendingEntry>> listPending(
            String groupName, StreamMessageId startId, StreamMessageId endId, int count) {
        return commandExecutor.reactive(() ->
                        instance.listPendingAsync(groupName, startId, endId, count))
                .filter(l -> !l.isEmpty());
    }

    public Mono<List<PendingEntry>> listPending(
            String groupName, String consumerName,
            StreamMessageId startId, StreamMessageId endId, int count) {
        return commandExecutor.reactive(() ->
                        instance.listPendingAsync(groupName, consumerName, startId, endId, count))
                .filter(l -> !l.isEmpty());
    }

    public Mono<List<PendingEntry>> listPending(
            String groupName, StreamMessageId startId, StreamMessageId endId,
            long idleTime, TimeUnit idleTimeUnit, int count) {
        return commandExecutor.reactive(() ->
                        instance.listPendingAsync(groupName, startId, endId, idleTime, idleTimeUnit, count))
                .filter(l -> !l.isEmpty());
    }

    public Mono<List<PendingEntry>> listPending(
            String groupName, String consumerName,
            StreamMessageId startId, StreamMessageId endId,
            long idleTime, TimeUnit idleTimeUnit, int count) {
        return commandExecutor.reactive(() ->
                        instance.listPendingAsync(groupName, consumerName, startId, endId, idleTime, idleTimeUnit, count))
                .filter(l -> !l.isEmpty());
    }

    public Mono<List<PendingEntry>> listPending(StreamPendingRangeArgs args) {
        return commandExecutor.reactive(() -> instance.listPendingAsync(args))
                .filter(l -> !l.isEmpty());
    }

    public Mono<List<StreamGroup>> listGroups() {
        return commandExecutor.reactive(instance::listGroupsAsync)
                .filter(l -> !l.isEmpty());
    }

    public Mono<List<StreamConsumer>> listConsumers(String groupName) {
        return commandExecutor.reactive(() -> instance.listConsumersAsync(groupName))
                .filter(l -> !l.isEmpty());
    }

    public Mono<Map<StreamMessageId, Map<K, V>>> range(
            StreamMessageId startId, StreamMessageId endId) {
        return commandExecutor.reactive(() -> instance.rangeAsync(startId, endId))
                .filter(m -> !m.isEmpty());
    }

    public Mono<Map<StreamMessageId, Map<K, V>>> range(
            int count, StreamMessageId startId, StreamMessageId endId) {
        return commandExecutor.reactive(() -> instance.rangeAsync(count, startId, endId))
                .filter(m -> !m.isEmpty());
    }

    public Mono<Map<StreamMessageId, Map<K, V>>> range(StreamRangeArgs args) {
        return commandExecutor.reactive(() -> instance.rangeAsync(args))
                .filter(m -> !m.isEmpty());
    }

    public Mono<Map<StreamMessageId, Map<K, V>>> rangeReversed(
            StreamMessageId startId, StreamMessageId endId) {
        return commandExecutor.reactive(() -> instance.rangeReversedAsync(startId, endId))
                .filter(m -> !m.isEmpty());
    }

    public Mono<Map<StreamMessageId, Map<K, V>>> rangeReversed(
            int count, StreamMessageId startId, StreamMessageId endId) {
        return commandExecutor.reactive(() -> instance.rangeReversedAsync(count, startId, endId))
                .filter(m -> !m.isEmpty());
    }

    public Mono<Map<StreamMessageId, Map<K, V>>> rangeReversed(StreamRangeArgs args) {
        return commandExecutor.reactive(() -> instance.rangeReversedAsync(args))
                .filter(m -> !m.isEmpty());
    }

    public Mono<Map<StreamMessageId, Map<K, V>>> pendingRange(
            String groupName, StreamMessageId startId, StreamMessageId endId, int count) {
        return commandExecutor.reactive(() ->
                        instance.pendingRangeAsync(groupName, startId, endId, count))
                .filter(m -> !m.isEmpty());
    }

    public Mono<Map<StreamMessageId, Map<K, V>>> pendingRange(
            String groupName, String consumerName,
            StreamMessageId startId, StreamMessageId endId, int count) {
        return commandExecutor.reactive(() ->
                        instance.pendingRangeAsync(groupName, consumerName, startId, endId, count))
                .filter(m -> !m.isEmpty());
    }

    public Mono<Map<StreamMessageId, Map<K, V>>> pendingRange(
            String groupName, StreamMessageId startId, StreamMessageId endId,
            long idleTime, TimeUnit idleTimeUnit, int count) {
        return commandExecutor.reactive(() ->
                        instance.pendingRangeAsync(groupName, startId, endId, idleTime, idleTimeUnit, count))
                .filter(m -> !m.isEmpty());
    }

    public Mono<Map<StreamMessageId, Map<K, V>>> pendingRange(
            String groupName, String consumerName,
            StreamMessageId startId, StreamMessageId endId,
            long idleTime, TimeUnit idleTimeUnit, int count) {
        return commandExecutor.reactive(() ->
                        instance.pendingRangeAsync(groupName, consumerName, startId, endId, idleTime, idleTimeUnit, count))
                .filter(m -> !m.isEmpty());
    }

    public Mono<Map<StreamMessageId, Map<K, V>>> claim(
            String groupName, String consumerName,
            long idleTime, TimeUnit idleTimeUnit, StreamMessageId... ids) {
        return commandExecutor.reactive(() ->
                        instance.claimAsync(groupName, consumerName, idleTime, idleTimeUnit, ids))
                .filter(m -> !m.isEmpty());
    }

}
