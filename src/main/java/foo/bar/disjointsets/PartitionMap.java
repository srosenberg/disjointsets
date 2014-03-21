package foo.bar.disjointsets;

import java.io.IOException;

import org.apache.hadoop.mapreduce.Mapper;

/**                                                                                                     
 * This phase determines if any given set of nodes is disjoint from _all_ other sets.
 * 
 * Here is a brief description of how this works.
 * All previous rep -> set mappings are simply passed through as long as card(set) > 1.
 * If card(set) == 1, then we emit set -> rep.  E.g., consider set {a, b} which would produce
 * the mappings: a -> {a,b}, b -> {a}, from ElectionReduce.  Here, we emit: a -> {a, b}, a -> {b}.
 * Assuming a, b do not belong to any other set, PartitionReduce will determine that the set {a, b} is disjoint.
 */                                                                                                     
public class PartitionMap<N extends Node<N>> extends Mapper<N, NodeSet<N>, N, NodeSet<N>> {     
	private NodeSet<N> VALUE; // inited at runtime
	
	@Override
	protected void setup(Context ctx) {
		VALUE = ElectionPartition.<N>createNodeSetInst(ctx.getConfiguration());
	}
	
	@Override 
    protected void map(N key, NodeSet<N> value, Context context) throws IOException, InterruptedException {
		VALUE.clear();
		
	    // swap n -> R with R -> n                                                                            
	    if (value.size() == 1) {
	    	N KEY = value.node();
	    	VALUE.add(key);
	    	context.write(KEY, VALUE); 
	    }
	    // otherwise, pass through                                                                    
	    else {
	    	context.write(key, value);  
	    }
	}                                                                                                    
}