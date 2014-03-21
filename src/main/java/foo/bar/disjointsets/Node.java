package foo.bar.disjointsets;

import org.apache.hadoop.io.WritableComparable;

/**
 * Supertype of all node types that are used in conjunction with <code>ElectionPartition</code>.
 * Note that this interface is simply used as a placeholder.  E.g., say you have a concrete node implementation <code>ConcreteNode</code>.
 * Then, the type becomes <code>Node&lt;ConcereteNode&gt;</code>.  Thus, <code>ConcreteNode</code> will have the following
 * method singatures: <code>public ConcreteNode clone()</code>, <code>public int compareTo(ConcereteNode n)</code>, etc.
 * 
 * @author srosenberg
 *
 * @param <N> actual type of Node
 */
public interface Node<N> extends WritableComparable<N>, Cloneable {
	N clone();
}
