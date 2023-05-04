/*******************************************************************************
 * Copyright 2021 valerio
 * 
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not
 * use this file except in compliance with the License.  You may obtain a copy
 * of the License at
 * 
 *   http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.  See the
 * License for the specific language governing permissions and limitations under
 * the License.
 ******************************************************************************/
package org.epos.router_framework.util;

import java.util.Map;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;

/**
 * Blocks on the map's value but management of the key is not fully thread-safe. However, it will work safely for use cases where the key is unique
 * (i.e. within some reasonable duration multiple attempts will not be carried out to set values against a specific key).
 * One suitable use case would therefore be using this map type to correlate messages based on unique correlation ids.
 * However, this map is only designed for use with a single value.
 * 
 * {@link Map} implementing {@link BlockingQueue}s values
 */
public class OneTimeKeyBlockingValueMap<K, V> {
	
	private final Map<K, BlockingQueue<V>> map = new ConcurrentHashMap<>();
	
    private synchronized BlockingQueue<V> ensureQueueExists(K key) {
    	return map.computeIfAbsent(key, k -> new ArrayBlockingQueue<>(1));
    }

    public boolean put(K key, V value, long timeout, TimeUnit timeUnit) throws InterruptedException {
        BlockingQueue<V> queue = ensureQueueExists(key);
	    synchronized (queue) {
	    	return queue.isEmpty() ? queue.offer(value, timeout, timeUnit) : false;   // NOSONAR - do not think rule 'java:S1125' should apply to ternary conditional return values
	    }
    }
      
    public boolean put(K key, V value) {
        BlockingQueue<V> queue = ensureQueueExists(key);
        synchronized (queue) {        	
        	return (queue.isEmpty()) ? queue.offer(value) : false; // NOSONAR - do not think rule 'java:S1125' should apply to ternary conditional return values
        }
    }
    
    /**
     * @param key K
     * @param timeout how long to wait before giving up, in units of unit
     * @param timeUnit a TimeUnit determining how to interpret the timeout parameter
     * @return V
     * @throws InterruptedException thread interrupted during polling of the queue value
     */
    public V get(K key, long timeout, TimeUnit timeUnit) throws InterruptedException 
    {
        BlockingQueue<V> queue = ensureQueueExists(key);
        V val = queue.poll(timeout, timeUnit);
        map.remove(key);
        return val;
    }
    
    /**
     * @param key K
     * @return V
     * @throws InterruptedException thread interrupted during polling of the queue value
     */
    public V get(K key) throws InterruptedException 
    {
        BlockingQueue<V> queue = ensureQueueExists(key);
        V val = queue.take();
        map.remove(key);
        return val;  		
    }
    
    public int size() {
    	return map.size();
    }

	@Override
	public String toString() {
		return "BlockingQueueMap " + super.toString() + " [map=" + map + "]";
	}
	
}
