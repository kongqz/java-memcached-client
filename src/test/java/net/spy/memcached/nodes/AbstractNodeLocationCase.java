package net.spy.memcached.nodes;

import java.util.Iterator;

import org.jmock.Mock;
import org.jmock.MockObjectTestCase;

import net.spy.memcached.util.KeyUtil;

public abstract class AbstractNodeLocationCase extends MockObjectTestCase {

	protected MemcachedNode[] nodes;
	protected Mock[] nodeMocks;
	protected NodeLocator locator;

	protected final void assertSequence(String k, int... seq) {
		int pos=0;
		for(Iterator<MemcachedNode> i=locator.getSequence(s(k));
			i.hasNext(); ) {
			assertSame("At position " + pos, nodes[seq[pos]], i.next());
			try {
				i.remove();
				fail("Allowed a removal from a sequence.");
			} catch(UnsupportedOperationException e) {
				// pass
			}
			pos++;
		}
		assertEquals("Incorrect sequence size for " + k, seq.length, pos);
	}

	protected byte[] s(String s) {
		return KeyUtil.getKeyBytes(s);
	}

	protected void setupNodes(int n) {
		nodes=new MemcachedNode[n];
		nodeMocks=new Mock[nodes.length];

		for(int i=0; i<nodeMocks.length; i++) {
			nodeMocks[i]=mock(MemcachedNode.class, "node#" + i);
			nodes[i]=(MemcachedNode)nodeMocks[i].proxy();
		}
	}
}