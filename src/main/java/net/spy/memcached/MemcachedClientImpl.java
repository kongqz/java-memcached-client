// Copyright (c) 2006  Dustin Sallings <dustin@spy.net>

package net.spy.memcached;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.SocketAddress;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import net.spy.SpyObject;
import net.spy.memcached.cas.CASResponse;
import net.spy.memcached.cas.CASValue;
import net.spy.memcached.io.MemcachedHighLevelIO;
import net.spy.memcached.ops.ConcatenationType;
import net.spy.memcached.ops.Mutator;
import net.spy.memcached.ops.Operation;
import net.spy.memcached.ops.StoreType;
import net.spy.memcached.transcoders.SerializingTranscoder;
import net.spy.memcached.transcoders.Transcoder;
import net.spy.memcached.util.KeyUtil;

/**
 * Implementation of MemcachedClient.
 * @see MemcachedClient
 */
public class MemcachedClientImpl extends SpyObject
	implements MemcachedClientWithTranscoder {

	private final MemcachedHighLevelIO conn;
    private final long operationTimeout;

	final Transcoder<Object> transcoder;

	/**
	 * Get a memcache client operating on the specified memcached locations.
	 *
	 * @param ia the memcached locations
	 * @throws IOException if connections cannot be established
	 */
	public MemcachedClientImpl(InetSocketAddress... ia) throws IOException {
		this(new DefaultConnectionFactory(), Arrays.asList(ia));
	}

	/**
	 * Get a memcache client over the specified memcached locations.
	 *
	 * @param addrs the socket addrs
	 * @throws IOException if connections cannot be established
	 */
	public MemcachedClientImpl(List<InetSocketAddress> addrs)
		throws IOException {
		this(new DefaultConnectionFactory(), addrs);
	}

	/**
	 * Get a memcache client over the specified memcached locations.
	 *
	 * @param bufSize read buffer size per connection (in bytes)
	 * @param addrs the socket addresses
	 * @throws IOException if connections cannot be established
	 */
	public MemcachedClientImpl(ConnectionFactory cf, List<InetSocketAddress> addrs)
		throws IOException {
		if(cf == null) {
			throw new NullPointerException("Connection factory required");
		}
		if(addrs == null) {
			throw new NullPointerException("Server list required");
		}
		if(addrs.isEmpty()) {
			throw new IllegalArgumentException(
				"You must have at least one server to connect to");
		}
		if(cf.getOperationTimeout() <= 0) {
			throw new IllegalArgumentException(
				"Operation timeout must be positive.");
		}
		transcoder=new SerializingTranscoder();
		conn=new MemcachedHighLevelIO(cf, addrs);
		operationTimeout = cf.getOperationTimeout();
	}

	/**
	 * (internal use) Add a raw operation to a numbered connection.
	 * This method is exposed for testing.
	 *
	 * @param which server number
	 * @param op the operation to perform
	 * @return the Operation
	 */
	Operation addOp(final String key, final Operation op) {
		return conn.addOp(KeyUtil.getKeyBytes(key), op);
	}

	private <T> Future<Boolean> asyncStore(StoreType storeType, String key,
					       int exp, T value, Transcoder<T> tc) {
		CachedData co=tc.encode(value);
		return conn.asyncStore(storeType, KeyUtil.getKeyBytes(key), exp, co,
			operationTimeout);
	}

	private Future<Boolean> asyncStore(StoreType storeType,
			String key, int exp, Object value) {
		return asyncStore(storeType, key, exp, value, transcoder);
	}

	private <T> Future<Boolean> asyncCat(
			ConcatenationType catType, long cas, String key,
			T value, Transcoder<T> tc) {
		CachedData co=tc.encode(value);
		return conn.asyncCat(catType, cas,
			KeyUtil.getKeyBytes(key), co, operationTimeout);
	}

	/* (non-Javadoc)
	 * @see net.spy.memcached.MemcachedClient#append(long, java.lang.String, java.lang.Object)
	 */
	public Future<Boolean> append(long cas, String key, Object val) {
		return append(cas, key, val, transcoder);
	}

	/* (non-Javadoc)
	 * @see net.spy.memcached.MemcachedClientWithTranscoder#append(long, java.lang.String, T, net.spy.memcached.transcoders.Transcoder)
	 */
	public <T> Future<Boolean> append(long cas, String key, T val,
			Transcoder<T> tc) {
		return asyncCat(ConcatenationType.append, cas, key, val, tc);
	}

	/* (non-Javadoc)
	 * @see net.spy.memcached.MemcachedClient#prepend(long, java.lang.String, java.lang.Object)
	 */
	public Future<Boolean> prepend(long cas, String key, Object val) {
		return prepend(cas, key, val, transcoder);
	}

	/* (non-Javadoc)
	 * @see net.spy.memcached.MemcachedClientWithTranscoder#prepend(long, java.lang.String, T, net.spy.memcached.transcoders.Transcoder)
	 */
	public <T> Future<Boolean> prepend(long cas, String key, T val,
			Transcoder<T> tc) {
		return asyncCat(ConcatenationType.prepend, cas, key, val, tc);
	}

	/* (non-Javadoc)
	 * @see net.spy.memcached.MemcachedClientWithTranscoder#asyncCAS(java.lang.String, long, T, net.spy.memcached.transcoders.Transcoder)
	 */
	public <T> Future<CASResponse> asyncCAS(String key, long casId, T value,
			Transcoder<T> tc) {
		CachedData co=tc.encode(value);
		return conn.asyncCAS(KeyUtil.getKeyBytes(key),
				casId, co, operationTimeout);
	}

	/* (non-Javadoc)
	 * @see net.spy.memcached.MemcachedClient#asyncCAS(java.lang.String, long, java.lang.Object)
	 */
	public Future<CASResponse> asyncCAS(String key, long casId, Object value) {
		return asyncCAS(key, casId, value, transcoder);
	}

	/* (non-Javadoc)
	 * @see net.spy.memcached.MemcachedClientWithTranscoder#cas(java.lang.String, long, T, net.spy.memcached.transcoders.Transcoder)
	 */
	public <T> CASResponse cas(String key, long casId, T value,
			Transcoder<T> tc) throws OperationTimeoutException {
		try {
			return asyncCAS(key, casId, value, tc).get(operationTimeout,
					TimeUnit.MILLISECONDS);
		} catch (InterruptedException e) {
			throw new RuntimeException("Interrupted waiting for value", e);
		} catch (ExecutionException e) {
			throw new RuntimeException("Exception waiting for value", e);
		} catch (TimeoutException e) {
			throw new OperationTimeoutException("Timeout waiting for value", e);
		}
	}

	/* (non-Javadoc)
	 * @see net.spy.memcached.MemcachedClient#cas(java.lang.String, long, java.lang.Object)
	 */
	public CASResponse cas(String key, long casId, Object value)
		throws OperationTimeoutException {
		return cas(key, casId, value, transcoder);
	}

	/* (non-Javadoc)
	 * @see net.spy.memcached.MemcachedClientWithTranscoder#add(java.lang.String, int, T, net.spy.memcached.transcoders.Transcoder)
	 */
	public <T> Future<Boolean> add(String key, int exp, T o, Transcoder<T> tc) {
		return asyncStore(StoreType.add, key, exp, o, tc);
	}

	/* (non-Javadoc)
	 * @see net.spy.memcached.MemcachedClient#add(java.lang.String, int, java.lang.Object)
	 */

	public Future<Boolean> add(String key, int exp, Object o) {
		return asyncStore(StoreType.add, key, exp, o, transcoder);
	}

	/* (non-Javadoc)
	 * @see net.spy.memcached.MemcachedClientWithTranscoder#set(java.lang.String, int, T, net.spy.memcached.transcoders.Transcoder)
	 */
	public <T> Future<Boolean> set(String key, int exp, T o, Transcoder<T> tc) {
		return asyncStore(StoreType.set, key, exp, o, tc);
	}

	/* (non-Javadoc)
	 * @see net.spy.memcached.MemcachedClient#set(java.lang.String, int, java.lang.Object)
	 */
	public Future<Boolean> set(String key, int exp, Object o) {
		return asyncStore(StoreType.set, key, exp, o, transcoder);
	}

	/* (non-Javadoc)
	 * @see net.spy.memcached.MemcachedClientWithTranscoder#replace(java.lang.String, int, T, net.spy.memcached.transcoders.Transcoder)
	 */
	public <T> Future<Boolean> replace(String key, int exp, T o,
		Transcoder<T> tc) {
		return asyncStore(StoreType.replace, key, exp, o, tc);
	}

	/* (non-Javadoc)
	 * @see net.spy.memcached.MemcachedClient#replace(java.lang.String, int, java.lang.Object)
	 */
	public Future<Boolean> replace(String key, int exp, Object o) {
		return asyncStore(StoreType.replace, key, exp, o, transcoder);
	}

	/* (non-Javadoc)
	 * @see net.spy.memcached.MemcachedClientWithTranscoder#asyncGet(java.lang.String, net.spy.memcached.transcoders.Transcoder)
	 */
	public <T> Future<T> asyncGet(final String key, final Transcoder<T> tc) {
		return conn.asyncGet(KeyUtil.getKeyBytes(key), tc, operationTimeout);
	}

	/* (non-Javadoc)
	 * @see net.spy.memcached.MemcachedClient#asyncGet(java.lang.String)
	 */
	public Future<Object> asyncGet(final String key) {
		return asyncGet(key, transcoder);
	}

	/* (non-Javadoc)
	 * @see net.spy.memcached.MemcachedClientWithTranscoder#asyncGets(java.lang.String, net.spy.memcached.transcoders.Transcoder)
	 */
	public <T> Future<CASValue<T>> asyncGets(final String key,
			final Transcoder<T> tc) {
		return conn.asyncGets(KeyUtil.getKeyBytes(key), tc, operationTimeout);
	}

	/* (non-Javadoc)
	 * @see net.spy.memcached.MemcachedClient#asyncGets(java.lang.String)
	 */
	public Future<CASValue<Object>> asyncGets(final String key) {
		return asyncGets(key, transcoder);
	}

	/* (non-Javadoc)
	 * @see net.spy.memcached.MemcachedClientWithTranscoder#gets(java.lang.String, net.spy.memcached.transcoders.Transcoder)
	 */
	public <T> CASValue<T> gets(String key, Transcoder<T> tc)
		throws OperationTimeoutException {
		try {
			return asyncGets(key, tc).get(
				operationTimeout, TimeUnit.MILLISECONDS);
		} catch (InterruptedException e) {
			throw new RuntimeException("Interrupted waiting for value", e);
		} catch (ExecutionException e) {
			throw new RuntimeException("Exception waiting for value", e);
		} catch (TimeoutException e) {
			throw new OperationTimeoutException("Timeout waiting for value", e);
		}
	}

	/* (non-Javadoc)
	 * @see net.spy.memcached.MemcachedClient#gets(java.lang.String)
	 */
	public CASValue<Object> gets(String key) throws OperationTimeoutException {
		return gets(key, transcoder);
	}

	/* (non-Javadoc)
	 * @see net.spy.memcached.MemcachedClientWithTranscoder#get(java.lang.String, net.spy.memcached.transcoders.Transcoder)
	 */
	public <T> T get(String key, Transcoder<T> tc)
		throws OperationTimeoutException {
		try {
			return asyncGet(key, tc).get(
				operationTimeout, TimeUnit.MILLISECONDS);
		} catch (InterruptedException e) {
			throw new RuntimeException("Interrupted waiting for value", e);
		} catch (ExecutionException e) {
			throw new RuntimeException("Exception waiting for value", e);
		} catch (TimeoutException e) {
			throw new OperationTimeoutException("Timeout waiting for value", e);
		}
	}

	/* (non-Javadoc)
	 * @see net.spy.memcached.MemcachedClient#get(java.lang.String)
	 */
	public Object get(String key) throws OperationTimeoutException {
		return get(key, transcoder);
	}

	/* (non-Javadoc)
	 * @see net.spy.memcached.MemcachedClientWithTranscoder#asyncGetBulk(java.util.Collection, net.spy.memcached.transcoders.Transcoder)
	 */
	public <T> Future<Map<String, T>> asyncGetBulk(Collection<String> keys,
		final Transcoder<T> tc) {
		return conn.asyncGetBulk(KeyUtil.getKeyBytes(keys), tc,
			operationTimeout);
	}

	/* (non-Javadoc)
	 * @see net.spy.memcached.MemcachedClient#asyncGetBulk(java.util.Collection)
	 */
	public Future<Map<String, Object>> asyncGetBulk(Collection<String> keys) {
		return asyncGetBulk(keys, transcoder);
	}

	/* (non-Javadoc)
	 * @see net.spy.memcached.MemcachedClientWithTranscoder#asyncGetBulk(net.spy.memcached.transcoders.Transcoder, java.lang.String)
	 */
	public <T> Future<Map<String, T>> asyncGetBulk(Transcoder<T> tc,
		String... keys) {
		return asyncGetBulk(Arrays.asList(keys), tc);
	}

	/* (non-Javadoc)
	 * @see net.spy.memcached.MemcachedClient#asyncGetBulk(java.lang.String)
	 */
	public Future<Map<String, Object>> asyncGetBulk(String... keys) {
		return asyncGetBulk(Arrays.asList(keys), transcoder);
	}

	/* (non-Javadoc)
	 * @see net.spy.memcached.MemcachedClientWithTranscoder#getBulk(java.util.Collection, net.spy.memcached.transcoders.Transcoder)
	 */
	public <T> Map<String, T> getBulk(Collection<String> keys, Transcoder<T> tc)
		throws OperationTimeoutException {
		try {
			return asyncGetBulk(keys, tc).get(
				operationTimeout, TimeUnit.MILLISECONDS);
		} catch (InterruptedException e) {
			throw new RuntimeException("Interrupted getting bulk values", e);
		} catch (ExecutionException e) {
			throw new RuntimeException("Failed getting bulk values", e);
		} catch (TimeoutException e) {
			throw new OperationTimeoutException(
				"Timeout waiting for bulkvalues", e);
        }
    }

	/* (non-Javadoc)
	 * @see net.spy.memcached.MemcachedClient#getBulk(java.util.Collection)
	 */
	public Map<String, Object> getBulk(Collection<String> keys)
		throws OperationTimeoutException {
		return getBulk(keys, transcoder);
	}

	/* (non-Javadoc)
	 * @see net.spy.memcached.MemcachedClientWithTranscoder#getBulk(net.spy.memcached.transcoders.Transcoder, java.lang.String)
	 */
	public <T> Map<String, T> getBulk(Transcoder<T> tc, String... keys)
		throws OperationTimeoutException {
		return getBulk(Arrays.asList(keys), tc);
	}

	/* (non-Javadoc)
	 * @see net.spy.memcached.MemcachedClient#getBulk(java.lang.String)
	 */
	public Map<String, Object> getBulk(String... keys)
		throws OperationTimeoutException {
		return getBulk(Arrays.asList(keys), transcoder);
	}

	/* (non-Javadoc)
	 * @see net.spy.memcached.MemcachedClient#getVersions()
	 */
	public Map<SocketAddress, String> getVersions() {
		return conn.getVersions(operationTimeout);
	}

	/* (non-Javadoc)
	 * @see net.spy.memcached.MemcachedClient#getStats()
	 */
	public Map<SocketAddress, Map<String, String>> getStats() {
		return getStats(null);
	}

	private Map<SocketAddress, Map<String, String>> getStats(final String arg) {
		return conn.getStats(arg, operationTimeout);
	}

	private long mutate(Mutator m, String key, int by, long def, int exp)
		throws OperationTimeoutException {
		return conn.mutate(m, KeyUtil.getKeyBytes(key), by, def, exp,
				operationTimeout);
	}

	/* (non-Javadoc)
	 * @see net.spy.memcached.MemcachedClient#incr(java.lang.String, int)
	 */
	public long incr(String key, int by) throws OperationTimeoutException {
		return mutate(Mutator.incr, key, by, 0, -1);
	}

	/* (non-Javadoc)
	 * @see net.spy.memcached.MemcachedClient#decr(java.lang.String, int)
	 */
	public long decr(String key, int by) throws OperationTimeoutException {
		return mutate(Mutator.decr, key, by, 0, -1);
	}

	private long mutateWithDefault(Mutator t, String key,
			int by, long def, int exp) throws OperationTimeoutException {
		long rv=mutate(t, key, by, def, exp);
		// The ascii protocol doesn't support defaults, so I added them
		// manually here.
		if(rv == -1) {
			Future<Boolean> f=asyncStore(StoreType.add,
					key, 0,	String.valueOf(def));
			try {
				if(f.get(operationTimeout, TimeUnit.MILLISECONDS)) {
					rv=def;
				} else {
					rv=mutate(t, key, by, 0, 0);
					assert rv != -1 : "Failed to mutate or init value";
				}
			} catch (InterruptedException e) {
				throw new RuntimeException("Interrupted waiting for store", e);
			} catch (ExecutionException e) {
				throw new RuntimeException("Failed waiting for store", e);
			} catch (TimeoutException e) {
				throw new OperationTimeoutException(
					"Timeout waiting to mutate or init value", e);
			}
		}
		return rv;
	}

	/* (non-Javadoc)
	 * @see net.spy.memcached.MemcachedClient#incr(java.lang.String, int, int)
	 */
	public long incr(String key, int by, int def)
		throws OperationTimeoutException {
		return mutateWithDefault(Mutator.incr, key, by, def, 0);
	}

	/* (non-Javadoc)
	 * @see net.spy.memcached.MemcachedClient#decr(java.lang.String, int, long)
	 */
	public long decr(String key, int by, long def)
		throws OperationTimeoutException {
		return mutateWithDefault(Mutator.decr, key, by, def, 0);
	}

	/* (non-Javadoc)
	 * @see net.spy.memcached.MemcachedClient#delete(java.lang.String, int)
	 */
	public Future<Boolean> delete(String key, int when) {
		return conn.delete(KeyUtil.getKeyBytes(key), when, operationTimeout);
	}

	/* (non-Javadoc)
	 * @see net.spy.memcached.MemcachedClient#delete(java.lang.String)
	 */
	public Future<Boolean> delete(String key) {
		return delete(key, 0);
	}

	/* (non-Javadoc)
	 * @see net.spy.memcached.MemcachedClient#flush(int)
	 */
	public Future<Boolean> flush(final int delay) {
		return conn.flush(delay, operationTimeout);
	}

	/* (non-Javadoc)
	 * @see net.spy.memcached.MemcachedClient#flush()
	 */
	public Future<Boolean> flush() {
		return flush(-1);
	}

	public Collection<SocketAddress> getAvailableServers() {
		return conn.getAvailableServers();
	}

	public Collection<SocketAddress> getUnavailableServers() {
		return conn.getUnavailableServers();
	}

	public void shutdown() {
		conn.shutdown();
	}

	public boolean shutdown(long timeout, TimeUnit unit) {
		return conn.shutdown(timeout, unit);
	}

	public boolean waitForQueues(long timeout, TimeUnit unit) {
		return conn.waitForQueues(timeout, unit);
	}
}
