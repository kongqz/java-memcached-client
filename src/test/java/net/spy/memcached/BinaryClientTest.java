package net.spy.memcached;

import net.spy.memcached.cas.CASValue;


/**
 * This test assumes a binary server is running on localhost:11211.
 */
public class BinaryClientTest extends ProtocolBaseCase {

	@Override
	protected void initClient() throws Exception {
		initClient(new BinaryConnectionFactory());
	}

	@Override
	protected String getExpectedVersionSource() {
		return "/127.0.0.1:11211";
	}

	public void testStats() {
		try {
			client.getStats();
		} catch(UnsupportedOperationException e) {
			// We don't have stats for the binary protocol yet.
		}
	}

	public void testCASAppendFail() throws Exception {
		final String key="append.key";
		assertTrue(client.set(key, 5, "test").get());
		CASValue<Object> casv = client.gets(key);
		assertFalse(client.append(casv.getCas() + 1, key, "es").get());
		assertEquals("test", client.get(key));
	}

	public void testCASAppendSuccess() throws Exception {
		final String key="append.key";
		assertTrue(client.set(key, 5, "test").get());
		CASValue<Object> casv = client.gets(key);
		assertTrue(client.append(casv.getCas(), key, "es").get());
		assertEquals("testes", client.get(key));
	}

	public void testCASPrependFail() throws Exception {
		final String key="append.key";
		assertTrue(client.set(key, 5, "test").get());
		CASValue<Object> casv = client.gets(key);
		assertFalse(client.prepend(casv.getCas() + 1, key, "es").get());
		assertEquals("test", client.get(key));
	}

	public void testCASPrependSuccess() throws Exception {
		final String key="append.key";
		assertTrue(client.set(key, 5, "test").get());
		CASValue<Object> casv = client.gets(key);
		assertTrue(client.prepend(casv.getCas(), key, "es").get());
		assertEquals("estest", client.get(key));
	}
}
