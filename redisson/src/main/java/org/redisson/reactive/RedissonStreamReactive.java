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
import org.redisson.api.stream.StreamMessageId;
import org.redisson.api.stream.StreamMultiReadArgs;
import org.redisson.api.stream.StreamMultiReadGroupArgs;
import org.redisson.api.stream.StreamReadArgs;
import org.redisson.api.stream.StreamReadGroupArgs;
import reactor.core.publisher.Mono;

import java.util.Map;

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

}
