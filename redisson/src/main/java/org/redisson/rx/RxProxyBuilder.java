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
package org.redisson.rx;

import io.reactivex.rxjava3.core.Completable;
import io.reactivex.rxjava3.core.Flowable;
import io.reactivex.rxjava3.core.Single;
import org.redisson.misc.ProxyBuilder;

import java.lang.reflect.Method;
import java.util.Map;
import java.util.concurrent.Callable;
import java.util.concurrent.CompletionStage;


/**
 * 
 * @author Nikita Koksharov
 *
 */
public class RxProxyBuilder {

    public static <T> T create(CommandRxExecutor commandExecutor, Object instance, Class<T> clazz) {
        return create(commandExecutor, instance, null, clazz);
    }

    public static <T> T create(CommandRxExecutor commandExecutor, Object instance, Object implementation, Class<T> clazz) {
        return ProxyBuilder.create((callable, instanceMethod) -> {
            Flowable<Object> flowable = commandExecutor.flowable((Callable<CompletionStage<Object>>) (Object) callable);
            if (isStreamReadMethod(instanceMethod)) {
                flowable = flowable.filter(r -> !(r instanceof Map && ((Map<?, ?>) r).isEmpty()));
            }

            if (instanceMethod.getReturnType() == Completable.class) {
                return flowable.ignoreElements();
            }
            if (instanceMethod.getReturnType() == Single.class) {
                return flowable.singleOrError();
            }
            return flowable.singleElement();
        }, instance, implementation, clazz, commandExecutor.getServiceManager());
    }

    private static boolean isStreamReadMethod(Method method) {
        if (!method.getName().equals("read") && !method.getName().equals("readGroup")) {
            return false;
        }

        Class<?>[] parameterTypes = method.getParameterTypes();
        if (parameterTypes.length == 0) {
            return false;
        }

        Package pkg = parameterTypes[parameterTypes.length - 1].getPackage();
        return pkg != null && "org.redisson.api.stream".equals(pkg.getName());
    }
    
}
