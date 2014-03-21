package foo.bar.disjointsets;

import java.io.IOException;

import org.apache.hadoop.mapreduce.Reducer;

/**                                                                                                     
 * Computes the union of all nodes which were grouped under the same key.
 */                                                                                                     
public class ElectionReduce<N extends Node<N>> extends Reducer<N, NodeSet<N>, N, NodeSet<N>> {
	private NodeSet<N> VALUE;  // inited at runtime
	
	@Override
	protected void setup(Context ctx) {
		VALUE = ElectionPartition.<N>createNodeSetInst(ctx.getConfiguration());
	}
	
	@Override                                                                                            
    protected void reduce(N key, Iterable<NodeSet<N>> values, Context context) throws IOException, InterruptedException {                                          
	    VALUE.clear(); 
	    
	    for(NodeSet<N> nodeSet : values ) {
	    	//N.B. nodeSet instance is reused but the nodes should be fresh (e.g., see PSguidSet.readFields)
	    	VALUE.addAll(nodeSet);
	    } 
	    // emit the union as value (same key)
	    context.write(key, VALUE);
	}     
}

