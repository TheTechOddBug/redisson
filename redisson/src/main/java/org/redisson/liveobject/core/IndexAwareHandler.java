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
package org.redisson.liveobject.core;

import java.lang.reflect.InvocationHandler;
import java.lang.reflect.Method;
import java.util.*;

import org.redisson.RedissonSetMultimap;
import org.redisson.api.RLiveObject;
import org.redisson.api.RMultimapAsync;
import org.redisson.command.CommandAsyncExecutor;
import org.redisson.command.CommandBatchService;
import org.redisson.liveobject.resolver.NamingScheme;

/**
 * Wraps a Redisson collection (RList/RSet) returned by a LiveObject getter
 * and synchronizes the @RIndex multimap after add/addAll/remove/removeAll/clear/set.
 * Unsupported mutations (removeIf, retainAll, replaceAll, sort) throw
 * UnsupportedOperationException.
 *
 * @author ngyngcphu
 */
public class IndexAwareHandler implements InvocationHandler {

    private final Collection<?> delegate;
    private final RLiveObject liveObj;
    private final NamingScheme ns;
    private final String indexName;
    private final CommandAsyncExecutor commandExecutor;

    public IndexAwareHandler(Collection<?> delegate, RLiveObject liveObj,
                              NamingScheme ns, String indexName, CommandAsyncExecutor commandExecutor) {
        this.delegate = delegate;
        this.liveObj = liveObj;
        this.ns = ns;
        this.indexName = indexName;
        this.commandExecutor = commandExecutor;
    }

    @Override
    public Object invoke(Object proxy, Method method, Object[] args) throws Throwable {
        String name = method.getName();

        switch (name) {
            case "iterator":
                return wrapIterator();
            case "listIterator":
                if (delegate instanceof List) {
                    return wrapListIterator(args);
                }
                return method.invoke(delegate, args);
            case "subList":
                return Collections.unmodifiableList(
                        ((List<?>) delegate).subList((Integer) args[0], (Integer) args[1]));
            case "contains": case "containsAll": case "size": case "isEmpty":
            case "get": case "indexOf": case "lastIndexOf": case "toArray":
            case "hashCode": case "equals": case "toString":
            case "stream": case "spliterator": case "parallelStream":
            case "forEach":
                return method.invoke(delegate, args);

            case "add": {
                Object result = method.invoke(delegate, args);
                // add(E) or add(int, E)
                if (args.length >= 1) {
                    Object element = args[args.length - 1];
                    if (element != null) {
                        syncAdd(element);
                    }
                }
                return result;
            }
            case "addAll": {
                Object result = method.invoke(delegate, args);
                // addAll(Collection) or addAll(int, Collection)
                Collection<?> toAdd;
                if (args.length == 2) {
                    toAdd = (Collection<?>) args[1];
                } else {
                    toAdd = (Collection<?>) args[0];
                }
                for (Object e : toAdd) {
                    if (e != null) {
                        syncAdd(e);
                    }
                }
                return result;
            }
            case "remove": {
                Object result = method.invoke(delegate, args);
                if (args.length == 1) {
                    // Disambiguate List.remove(int) vs Collection.remove(Object) by parameter type
                    if (delegate instanceof List && method.getParameterTypes().length == 1
                            && method.getParameterTypes()[0] == int.class) {
                        Object removed = result;
                        if (removed != null) {
                            syncRemove(removed);
                        }
                    } else if (Boolean.TRUE.equals(result) && args[0] != null) {
                        syncRemove(args[0]);
                    }
                }
                return result;
            }
            case "removeAll": {
                Object result = method.invoke(delegate, args);
                if (args.length == 1 && Boolean.TRUE.equals(result)) {
                    for (Object e : (Collection<?>) args[0]) {
                        if (e != null) {
                            syncRemove(e);
                        }
                    }
                }
                return result;
            }
            case "clear": {
                Object result = method.invoke(delegate, args);
                syncClear();
                return result;
            }
            case "set": {
                Object result = method.invoke(delegate, args);
                if (args.length >= 2) {
                    CommandBatchService ce = new CommandBatchService(commandExecutor);
                    RMultimapAsync<Object, Object> map = new RedissonSetMultimap<>(ns.getCodec(), ce, indexName);
                    if (result != null) {
                        map.removeAsync(resolveKey(result), liveObj.getLiveObjectId());
                    }
                    if (args[1] != null) {
                        map.putAsync(resolveKey(args[1]), liveObj.getLiveObjectId());
                    }
                    ce.execute();
                }
                return result;
            }

            default:
                throw new UnsupportedOperationException(
                        method.getName() + " is not supported on indexed collections. "
                        + "Use add/remove/iterator() instead.");
        }
    }

    private void syncAdd(Object element) {
        CommandBatchService ce = new CommandBatchService(commandExecutor);
        new RedissonSetMultimap<>(ns.getCodec(), ce, indexName)
                .putAsync(resolveKey(element), liveObj.getLiveObjectId());
        ce.execute();
    }

    private void syncRemove(Object element) {
        CommandBatchService ce = new CommandBatchService(commandExecutor);
        new RedissonSetMultimap<>(ns.getCodec(), ce, indexName)
                .removeAsync(resolveKey(element), liveObj.getLiveObjectId());
        ce.execute();
    }

    private void syncClear() {
        CommandBatchService ce = new CommandBatchService(commandExecutor);
        new RedissonSetMultimap<>(ns.getCodec(), ce, indexName)
                .fastRemoveValueAsync(liveObj.getLiveObjectId());
        ce.execute();
    }

    private Object resolveKey(Object element) {
        if (element instanceof RLiveObject) {
            return ((RLiveObject) element).getLiveObjectId();
        }
        return element;
    }

    private Iterator<Object> wrapIterator() {
        Iterator<Object> it = ((Collection<Object>) delegate).iterator();
        return new Iterator<Object>() {
            Object lastReturned;
            public boolean hasNext() {
                return it.hasNext();
            }
            public Object next() {
                lastReturned = it.next();
                return lastReturned;
            }
            public void remove() {
                it.remove();
                if (lastReturned != null) {
                    syncRemove(lastReturned);
                    lastReturned = null;
                }
            }
        };
    }

    private ListIterator<Object> wrapListIterator(Object[] args) {
        ListIterator<Object> it;
        if (args != null && args.length > 0) {
            it = ((List<Object>) delegate).listIterator((Integer) args[0]);
        } else {
            it = ((List<Object>) delegate).listIterator();
        }
        return new ListIterator<Object>() {
            Object lastReturned;
            public boolean hasNext() {
                return it.hasNext();
            }
            public boolean hasPrevious() {
                return it.hasPrevious();
            }
            public Object next() {
                lastReturned = it.next();
                return lastReturned;
            }
            public Object previous() {
                lastReturned = it.previous();
                return lastReturned;
            }
            public int nextIndex() {
                return it.nextIndex();
            }
            public int previousIndex() {
                return it.previousIndex();
            }
            public void remove() {
                it.remove();
                if (lastReturned != null) {
                    syncRemove(lastReturned);
                    lastReturned = null;
                }
            }
            public void set(Object e) {
                it.set(e);
                if (lastReturned != null) {
                    CommandBatchService ce = new CommandBatchService(commandExecutor);
                    RMultimapAsync<Object, Object> map = new RedissonSetMultimap<>(ns.getCodec(), ce, indexName);
                    map.removeAsync(resolveKey(lastReturned), liveObj.getLiveObjectId());
                    if (e != null) {
                        map.putAsync(resolveKey(e), liveObj.getLiveObjectId());
                    }
                    ce.execute();
                    lastReturned = e;
                }
            }
            public void add(Object e) {
                it.add(e);
                if (e != null) {
                    syncAdd(e);
                }
                lastReturned = null;
            }
        };
    }
}
