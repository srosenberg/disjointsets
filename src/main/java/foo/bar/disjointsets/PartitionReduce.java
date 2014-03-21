package foo.bar.disjointsets;

import java.io.IOException;
import java.util.HashMap;

import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Reducer;

/**                                                                                                     
 * Determine if the given value denotes a set that is disjoint from _all_ other sets.  This occurs when each element in the given value
 * occurs exactly twice (see PartitionMap).
 *
 */                                                                                                     
public class PartitionReduce<N extends Node<N>> extends Reducer<N, NodeSet<N>, NullWritable, NodeSet<N>> implements MRDefs {
    private NodeSet<N> VALUE; // inited at runtime
    // given an element, return how many times it occurs in the reduce's value list
    private HashMap<N, Integer> elem2occurrences = new HashMap<N, Integer>(); 
    
	@Override
	protected void setup(Context ctx) {
		VALUE = ElectionPartition.<N>createNodeSetInst(ctx.getConfiguration());
	}
		
    @Override
    protected void reduce(N key, Iterable<NodeSet<N>> values, Context context) throws IOException, InterruptedException {                                         
    	elem2occurrences = new HashMap<N, Integer>();  
    	VALUE.clear();
    	
	    N representative = key;
	    // Inject a 1 for the representative, so it counts itself twice.                                             
	    elem2occurrences.put(representative, 1);                  

	    for (NodeSet<N> nodeSet : values) {     
	    	// count how many times each node appears
			for (N node : nodeSet) {   
				// heartbeat
				context.getCounter(MiscCounters.HEARTBEAT).increment(1);
			    // previous number of occurrences if any
			    Integer count = elem2occurrences.get(node);
			    
				if (count != null)                                                              
					elem2occurrences.put(node, count+1);                                 
			    else                                                                                        
			    	elem2occurrences.put(node, 1);                                                                     
		    } 
	    }
	    // Our new set; assume it's disjoint initially
	    VALUE.setDisjoint(true);
	    VALUE.addAll(elem2occurrences.keySet());

	    // set is disjoint iff count == 2 for each element
	    for (int count : elem2occurrences.values()) {                                                          
			if (count != 2) {
				// value is not a disjoint set because at least one of its nodes is a member of
				// another set
				VALUE.setDisjoint(false);                                           
			    break;                                                                                      
			}  
	    }
	    //N.B. These counters are mandatory!                                                                                                                                                                                                 
	    if (VALUE.getDisjoint())                                                                        
	    	context.getCounter(DisjointSetsCounter.DISJOINT).increment(1);                                             
	    else                                                                                              
	    	context.getCounter(DisjointSetsCounter.NON_DISJOINT).increment(1);

	    // emit
	    context.write(NullWritable.get(), VALUE);                                                                        
	}     
}