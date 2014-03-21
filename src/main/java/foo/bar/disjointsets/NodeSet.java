package foo.bar.disjointsets;

import java.util.Set;

import org.apache.hadoop.io.Writable;

/**
 * Denotes a hadoop-writable set of nodes.  This interface is intended for use with Election/Partition algorithm.
 * 
 * The node type must implement Comparable; this is needed for election of set representatives.
 * 
 * @author srosenberg
 *
 * @param <N>  actual node type 
 */
public interface NodeSet<N extends Node<N>> extends Set<N>, Writable, Cloneable {
	/**
	 * @return whether or not this node set is disjoint from all other known node sets
	 */
	boolean getDisjoint();
	
	/**
	 * updates "disjointedness" of this node set
	 */
	void setDisjoint(boolean f);
	
	/**
	 * @return representative of this node set
	 */
	N rep();
	
	/**
	 * @return the node if this is a singleton set
	 * @throws UnsupportedOperationException  if size() != 1
	 */
	N node() throws UnsupportedOperationException;
	
	/**
	 * 
	 * @return new instance, n, such that n != this and n.equals(this)
	 */
	NodeSet<N> clone();

}

