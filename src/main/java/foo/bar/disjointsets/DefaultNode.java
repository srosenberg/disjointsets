package foo.bar.disjointsets;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.io.Serializable;

import org.apache.hadoop.io.Text;

import foo.bar.utils.ObjectSerializer;

public class DefaultNode<N extends Serializable & Comparable<N>> implements Node<DefaultNode<N>>, Serializable {
	/**
	 * 
	 */
	private static final long serialVersionUID = 1L;

	private N node;
	
	private final static Text DUMMY = new Text();
	
	/**
	 * This private constructor is needed for hadoop map/reduce classes which will invoke it through reflection.
	 */
	@SuppressWarnings("unused")
	private DefaultNode() {}
	
	public DefaultNode(N node) {
		this.node = node;
	}
	
	@Override
	public void write(DataOutput out) throws IOException {
		String ser = ObjectSerializer.serialize(node);
		DUMMY.set(ser);
		DUMMY.write(out);
	}

	@SuppressWarnings("unchecked")
	@Override
	public void readFields(DataInput in) throws IOException {
		DUMMY.readFields(in);
		node = (N) ObjectSerializer.deserialize(DUMMY.toString());
	}

	@Override
	public int compareTo(DefaultNode<N> o) {
		return node.compareTo(o.node);
	}
	
	@Override
	public boolean equals(Object o) {
		if (o == this)
			return true;

		if (!(o instanceof DefaultNode))
			return false;
		
		return node.equals(((DefaultNode)o).node);
	}
	
	@Override
	public int hashCode() {
		return node.hashCode();
	}
	
	@Override
	public DefaultNode<N> clone() {
		throw new RuntimeException(new CloneNotSupportedException());
	}
	
	@Override
	public String toString() {
		return node.toString();
	}
	
}
