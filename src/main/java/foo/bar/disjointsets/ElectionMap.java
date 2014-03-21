package foo.bar.disjointsets;

import java.io.IOException;

import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Mapper;

/**
 * Elects a representative of each set.
 * 
 * <p>
 * The input to this mapper consists of sets of nodes.
 *
 * The output is defined with respect to each set of <code>nodes</code>:
 * <OL>
 * 	 <LI> Emit key/value pair: <code>rep, nodes</code> </LI>
 *   <LI> For each n in nodes such that n != rep, emit key/value pair: <code>n, {rep}</code> </LI> 
 * </OL>                                          
 * Ignores known DISJOINT sets.                                                                         
 *
 */        
public class ElectionMap<N extends Node<N>> extends Mapper<NullWritable, NodeSet<N>, N, NodeSet<N>> implements MRDefs {         
	private NodeSet<N> VALUE; // inited at runtime
	
	@Override
	protected void setup(Context ctx) {
		VALUE = ElectionPartition.createNodeSetInst(ctx.getConfiguration());
	}

	@Override                                                                                            
    protected void map(NullWritable key, NodeSet<N> value, Context context) throws IOException,InterruptedException { 
		VALUE.clear();
		
	    // If it was a disjoint output from the last iteration, then don't                                
	    // continue to propagate it.                                                                      
	    if (value.getDisjoint()) {                                                                      
	    	context.getCounter(DisjointSetsCounter.DISJOINT).increment(1);                                             
			return;                                                                                        
	    }                                                                                                 
	    // otherwise, we have an "open" item
	    context.getCounter(DisjointSetsCounter.NON_DISJOINT).increment(1);                                                    
             
	    N representative = value.rep();
	    // emit rep -> nodes
	    context.write(representative, value);  
	    // emit node -> rep, excluding rep -> rep
	    for (N node : value) {       
	    	if (node != representative) {
	    		VALUE.add(representative);
	    		context.write(node, VALUE);
	    	}
	    }
	}  
}
