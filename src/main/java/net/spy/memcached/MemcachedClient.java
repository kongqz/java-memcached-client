package net.spy.memcached;

import java.net.SocketAddress;
import java.util.Collection;
import java.util.Map;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

import net.spy.memcached.cas.CASResponse;
import net.spy.memcached.cas.CASValue;

/**
 * Basic interface for talking to memcached.
 *
 * <h2>Basic usage</h2>
 *
 * <pre>
 *  MemcachedClient c=new MemcachedClientImpl(
 *      new InetSocketAddress("hostname", portNum));
 *
 *  // Store a value (async) for one hour
 *  c.set("someKey", 3600, someObject);
 *  // Retrieve a value.
 *  Object myObject=c.get("someKey");
 *  </pre>
 *
 *  <h2>Advanced Usage</h2>
 *
 *  <p>
 *   MemcachedClient may be processing a great deal of asynchronous messages or
 *   possibly dealing with an unreachable memcached, which may delay processing.
 *   If a memcached is disabled, for example, MemcachedConnection will continue
 *   to attempt to reconnect and replay pending operations until it comes back
 *   up.  To prevent this from causing your application to hang, you can use
 *   one of the asynchronous mechanisms to time out a request and cancel the
 *   operation to the server.
 *  </p>
 *
 *  <pre>
 *  // Get a memcached client connected to several servers
 *  MemcachedClient c=new MemcachedClientImpl(
 *      AddrUtil.getAddresses("server1:11211 server2:11211"));
 *
 *  // Try to get a value, for up to 5 seconds, and cancel if it doesn't return
 *  Object myObj=null;
 *  Future&lt;Object&gt; f=c.asyncGet("someKey");
 *  try {
 *      myObj=f.get(5, TimeUnit.SECONDS);
 *  } catch(TimeoutException e) {
 *      // Since we don't need this, go ahead and cancel the operation.  This
 *      // is not strictly necessary, but it'll save some work on the server.
 *      f.cancel();
 *      // Do other timeout related stuff
 *  }
 * </pre>
 */
public interface MemcachedClient {

	/**
	 * Get the addresses of available servers.
	 *
	 * <p>
	 * This is based on a snapshot in time so shouldn't be considered
	 * completely accurate, but is a useful for getting a feel for what's
	 * working and what's not working.
	 * </p>
	 */
	Collection<SocketAddress> getAvailableServers();

	/**
	 * Get the addresses of unavailable servers.
	 *
	 * <p>
	 * This is based on a snapshot in time so shouldn't be considered
	 * completely accurate, but is a useful for getting a feel for what's
	 * working and what's not working.
	 * </p>
	 */
	Collection<SocketAddress> getUnavailableServers();

	/**
	 * Append to an existing value in the cache.
	 *
	 * @param cas cas identifier (ignored in the ascii protocol)
	 * @param key the key to whose value will be appended
	 * @param val the value to append
	 * @return a future indicating success
	 */
	Future<Boolean> append(long cas, String key, Object val);

	/**
	 * Prepend to an existing value in the cache.
	 *
	 * @param cas cas identifier (ignored in the ascii protocol)
	 * @param key the key to whose value will be prepended
	 * @param val the value to append
	 * @return a future indicating success
	 */
	Future<Boolean> prepend(long cas, String key, Object val);

	/**
	 * Asynchronous CAS operation using the default transcoder.
	 *
	 * @param key the key
	 * @param casId the CAS identifier (from a gets operation)
	 * @param value the new value
	 * @return a future that will indicate the status of the CAS
	 */
	Future<CASResponse> asyncCAS(String key, long casId, Object value);

	/**
	 * Perform a synchronous CAS operation with the default transcoder.
	 *
	 * @param key the key
	 * @param casId the CAS identifier (from a gets operation)
	 * @param value the new value
	 * @return a CASResponse
	 * @throws OperationTimeoutException if the global operation timeout is
	 *         exceeded
	 */
	CASResponse cas(String key, long casId, Object value)
			throws OperationTimeoutException;

	/**
	 * Add an object to the cache (using the default transcoder)
	 * iff it does not exist already.
	 *
	 * <p>
	 * The <code>exp</code> value is passed along to memcached exactly as
	 * given, and will be processed per the memcached protocol specification:
	 * </p>
	 *
	 * <blockquote>
	 * <p>
	 * The actual value sent may either be
	 * Unix time (number of seconds since January 1, 1970, as a 32-bit
	 * value), or a number of seconds starting from current time. In the
	 * latter case, this number of seconds may not exceed 60*60*24*30 (number
	 * of seconds in 30 days); if the number sent by a client is larger than
	 * that, the server will consider it to be real Unix time value rather
	 * than an offset from current time.
	 * </p>
	 * </blockquote>
	 *
	 * @param key the key under which this object should be added.
	 * @param exp the expiration of this object
	 * @param o the object to store
	 * @return a future representing the processing of this operation
	 */

	Future<Boolean> add(String key, int exp, Object o);

	/**
	 * Set an object in the cache (using the default transcoder)
	 * regardless of any existing value.
	 *
	 * <p>
	 * The <code>exp</code> value is passed along to memcached exactly as
	 * given, and will be processed per the memcached protocol specification:
	 * </p>
	 *
	 * <blockquote>
	 * <p>
	 * The actual value sent may either be
	 * Unix time (number of seconds since January 1, 1970, as a 32-bit
	 * value), or a number of seconds starting from current time. In the
	 * latter case, this number of seconds may not exceed 60*60*24*30 (number
	 * of seconds in 30 days); if the number sent by a client is larger than
	 * that, the server will consider it to be real Unix time value rather
	 * than an offset from current time.
	 * </p>
	 * </blockquote>
	 *
	 * @param key the key under which this object should be added.
	 * @param exp the expiration of this object
	 * @param o the object to store
	 * @return a future representing the processing of this operation
	 */
	Future<Boolean> set(String key, int exp, Object o);

	/**
	 * Replace an object with the given value (transcoded with the default
	 * transcoder) iff there is already a value for the given key.
	 *
	 * <p>
	 * The <code>exp</code> value is passed along to memcached exactly as
	 * given, and will be processed per the memcached protocol specification:
	 * </p>
	 *
	 * <blockquote>
	 * <p>
	 * The actual value sent may either be
	 * Unix time (number of seconds since January 1, 1970, as a 32-bit
	 * value), or a number of seconds starting from current time. In the
	 * latter case, this number of seconds may not exceed 60*60*24*30 (number
	 * of seconds in 30 days); if the number sent by a client is larger than
	 * that, the server will consider it to be real Unix time value rather
	 * than an offset from current time.
	 * </p>
	 * </blockquote>
	 *
	 * @param key the key under which this object should be added.
	 * @param exp the expiration of this object
	 * @param o the object to store
	 * @return a future representing the processing of this operation
	 */
	Future<Boolean> replace(String key, int exp, Object o);

	/**
	 * Get the given key asynchronously and decode with the default
	 * transcoder.
	 *
	 * @param key the key to fetch
	 * @return a future that will hold the return value of the fetch
	 */
	Future<Object> asyncGet(final String key);

	/**
	 * Gets (with CAS support) the given key asynchronously and decode using
	 * the default transcoder.
	 *
	 * @param key the key to fetch
	 * @return a future that will hold the return value of the fetch
	 */
	Future<CASValue<Object>> asyncGets(final String key);

	/**
	 * Gets (with CAS support) with a single key using the default transcoder.
	 *
	 * @param key the key to get
	 * @return the result from the cache and CAS id (null if there is none)
	 * @throws OperationTimeoutException if the global operation timeout is
	 *         exceeded
	 */
	CASValue<Object> gets(String key) throws OperationTimeoutException;

	/**
	 * Get with a single key and decode using the default transcoder.
	 *
	 * @param key the key to get
	 * @return the result from the cache (null if there is none)
	 * @throws OperationTimeoutException if the global operation timeout is
	 *         exceeded
	 */
	Object get(String key) throws OperationTimeoutException;

	/**
	 * Asynchronously get a bunch of objects from the cache and decode them
	 * with the given transcoder.
	 *
	 * @param keys the keys to request
	 * @return a Future result of that fetch
	 */
	Future<Map<String, Object>> asyncGetBulk(Collection<String> keys);

	/**
	 * Varargs wrapper for asynchronous bulk gets with the default transcoder.
	 *
	 * @param keys one more more keys to get
	 * @return the future values of those keys
	 */
	Future<Map<String, Object>> asyncGetBulk(String... keys);

	/**
	 * Get the values for multiple keys from the cache.
	 *
	 * @param keys the keys
	 * @return a map of the values (for each value that exists)
	 * @throws OperationTimeoutException if the global operation timeout is
	 *         exceeded
	 */
	Map<String, Object> getBulk(Collection<String> keys)
			throws OperationTimeoutException;

	/**
	 * Get the values for multiple keys from the cache.
	 *
	 * @param keys the keys
	 * @return a map of the values (for each value that exists)
	 * @throws OperationTimeoutException if the global operation timeout is
	 *         exceeded
	 */
	Map<String, Object> getBulk(String... keys)
			throws OperationTimeoutException;

	/**
	 * Get the versions of all of the connected memcacheds.
	 */
	Map<SocketAddress, String> getVersions();

	/**
	 * Get all of the stats from all of the connections.
	 */
	Map<SocketAddress, Map<String, String>> getStats();

	/**
	 * Increment the given key by the given amount.
	 *
	 * @param key the key
	 * @param by the amount to increment
	 * @return the new value (-1 if the key doesn't exist)
	 * @throws OperationTimeoutException if the global operation timeout is
	 *         exceeded
	 */
	long incr(String key, int by) throws OperationTimeoutException;

	/**
	 * Decrement the given key by the given value.
	 *
	 * @param key the key
	 * @param by the value
	 * @return the new value (-1 if the key doesn't exist)
	 * @throws OperationTimeoutException if the global operation timeout is
	 *         exceeded
	 */
	long decr(String key, int by) throws OperationTimeoutException;

	/**
	 * Increment the given counter, returning the new value.
	 *
	 * @param key the key
	 * @param by the amount to increment
	 * @param def the default value (if the counter does not exist)
	 * @return the new value, or -1 if we were unable to increment or add
	 * @throws OperationTimeoutException if the global operation timeout is
	 *         exceeded
	 */
	long incr(String key, int by, int def) throws OperationTimeoutException;

	/**
	 * Decrement the given counter, returning the new value.
	 *
	 * @param key the key
	 * @param by the amount to decrement
	 * @param def the default value (if the counter does not exist)
	 * @return the new value, or -1 if we were unable to decrement or add
	 * @throws OperationTimeoutException if the global operation timeout is
	 *         exceeded
	 */
	long decr(String key, int by, long def) throws OperationTimeoutException;

	/**
	 * Delete the given key from the cache.
	 *
	 * @param key the key to delete
	 * @param when when the deletion should take effect
	 */
	Future<Boolean> delete(String key, int when);

	/**
	 * Shortcut to delete that will immediately delete the item from the cache.
	 */
	Future<Boolean> delete(String key);

	/**
	 * Flush all caches from all servers with a delay of application.
	 */
	Future<Boolean> flush(final int delay);

	/**
	 * Flush all caches from all servers immediately.
	 */
	Future<Boolean> flush();

	/**
	 * Shut down immediately.
	 */
	void shutdown();

	/**
	 * Shut down this client gracefully.
	 */
	boolean shutdown(long timeout, TimeUnit unit);

	/**
	 * Wait for the queues to die down.
	 */
	boolean waitForQueues(long timeout, TimeUnit unit);

}
