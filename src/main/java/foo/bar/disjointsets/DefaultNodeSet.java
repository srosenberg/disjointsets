package foo.bar.disjointsets;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.AbstractSet;
import java.util.Iterator;
import java.util.TreeSet;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableFactories;

/**
 * The default implementation of a <code>NodeSet</code>.  This implementation essentially wraps around <code>TreeSet</code>.
 * 
 * @author srosenberg
 *
 * @param <N>
 */
public class DefaultNodeSet<N extends Node<N>> extends AbstractSet<N> implements NodeSet<N>, Writable {
	protected TreeSet<N> nodes;
	protected boolean disjoint = false;
	
	private final static Text DUMMY = new Text();

	public DefaultNodeSet() {
		this(null);
	}
	
	
	/**
	 * Construct a singleton node with the given node.  If the node happens to be <code>null</code>, then the empty
	 * node set is constructed. 
	 * 
	 * @param nodeClass
	 * @param node
	 */
	public DefaultNodeSet(N node) {
		nodes = new TreeSet<N>();
		if (node != null) {
			nodes.add(node);
		}
	}
	
	@Override
	public void write(DataOutput out) throws IOException {
		// write size
		out.writeInt(nodes.size());
		if (nodes.size() > 0) {
			// write node class
			DUMMY.set(nodes.first().getClass().getCanonicalName());
			DUMMY.write(out);
		}
		// write each node
		for (N node : nodes) {
			node.write(out);
		}
		//  write disjoint flag
		out.writeBoolean(disjoint);
	}

	@SuppressWarnings("unchecked")
	@Override
	public void readFields(DataInput in) throws IOException {
		// read size
		int size = in.readInt();
		nodes = new TreeSet<N>();
		Class nodeClass = null;
		
		if (size > 0) {
			// read node class
			DUMMY.readFields(in);
			
			try {
				nodeClass = Class.forName(DUMMY.toString());
			} catch (Exception ex) {
				throw new IOException("Could not deserialize node class", ex);
			}
		}
		// read each node
		N node;
		
		for (int i = 0; i < size; i++) {
			node = (N) WritableFactories.newInstance(nodeClass);
			node.readFields(in);
			nodes.add(node);
		}
		// read disjoint flag
		disjoint = in.readBoolean();
	}

	@Override
	public boolean getDisjoint() {
		return disjoint;
	}

	@Override
	public void setDisjoint(boolean f) {
		this.disjoint = f;
	}

	@Override
	public N rep() {
		return nodes.first();
	}

	@Override
	public N node() throws UnsupportedOperationException {
		if (nodes.size() != 1) {
			throw new UnsupportedOperationException("Not a singleton set");
		}
		return nodes.first();
	}

	@Override
	public Iterator<N> iterator() {
		return nodes.iterator();
	}

	@Override
	public int size() {
		return nodes.size();
	}
	
	@Override
	public boolean add(N node) {
		return nodes.add(node);
	}
	
	@Override
	public boolean contains(Object node) {
		return nodes.contains(node);
	}
	
	@Override
	public void clear() {
		nodes.clear();	
	}
	
	@Override
	public boolean remove(Object node) {
		return nodes.remove(node);
	}
	
	@SuppressWarnings("unchecked")
	@Override
	public boolean equals(Object o) {
		if (o == this)
			return true;

		if (!(o instanceof DefaultNodeSet))
			return false;
		
		DefaultNodeSet<N> other = (DefaultNodeSet<N>)o;

		return this.disjoint == other.disjoint && this.nodes.equals(other.nodes);
	}
	
	@Override
	public int hashCode() {
		// ignore msb in order to factor in 'disjoint'
		int result = nodes.hashCode() << 1;
		return disjoint ? (result | 1) : result;
	}
	
	/**
	 *  Returns a shallow clone of this node set.  That is, the actual nodes are not clone
	 */
	@Override
	public DefaultNodeSet<N> clone() {
		DefaultNodeSet<N> result = new DefaultNodeSet<N>();
		result.nodes = new TreeSet<N>();
		result.nodes.addAll(nodes);
		result.disjoint = disjoint;
		
		return result;
	}
	
	@Override
	public String toString() {
		StringBuilder sb = new StringBuilder();
		
	    if (disjoint) {
	    	sb.append("DISJOINT: ");
	    } else {
	    	sb.append("NON_DISJOINT: ");
	    }
	    // output cardinality for ease of data mining using coreutils (e.g., awk)
	    sb.append(nodes.size());  sb.append(" : ");
	    
	    String prefix = "";

	    for (N node : nodes) {
	    	sb.append(prefix);  sb.append(node);
	    	prefix = ",";
	    }
	    return sb.toString();
	}
}
