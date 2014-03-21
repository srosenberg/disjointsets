package foo.bar.disjointsets;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Iterator;
import java.util.List;
import java.util.Set;

import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.log4j.Logger;
import org.jgrapht.UndirectedGraph;
import org.jgrapht.alg.ConnectivityInspector;
import org.jgrapht.graph.DefaultEdge;
import org.jgrapht.graph.SimpleGraph;

/**
 * Single reducer which computes connected components in-memory.
 * <p>
 * This is essentially a drop-in replacement for <code>PartitionReduce</code>.
 * We use the (default) identity mapper.
 * 
 * @author srosenberg
 *
 * @param <N> node type
 */

public class ConnectedComponentsReduce<N extends Node<N>> extends Reducer<N, NodeSet<N>, NullWritable, NodeSet<N>> implements MRDefs {
	private NodeSet<N> VALUE; // inited at runtime
	private int maxNumSets;
	private ArrayList<NodeSet<N>> sets;
	private final static Logger log = Logger.getLogger(ConnectedComponentsReduce.class);
	
	@Override
	protected void setup(Context ctx) {
		VALUE = ElectionPartition.<N>createNodeSetInst(ctx.getConfiguration());
		VALUE.setDisjoint(true);
		
		maxNumSets = ctx.getConfiguration().getInt(ElectionPartition.THRESHOLD, ElectionPartition.DEFAULT_THRESHOLD);
		sets = new ArrayList<NodeSet<N>>(maxNumSets);
		log.info("Initialized reducer; free heap: " + Runtime.getRuntime().freeMemory()); 
	}
	
	@Override                                                                                            
    protected void reduce(N key, Iterable<NodeSet<N>> value, Context context) throws IOException, InterruptedException {
		VALUE.clear();
		// using 'clone' as a substitute for 'new'
		NodeSet<N> set = VALUE.clone();
		// clone the key, otherwise it might be overwritten by next call to reduce
		set.add(key.clone());
		// Note, we don't need to clone the elements; this is typically done in NodeSet, e.g.,
		// see DefaultNodeSet
		for (NodeSet<N> s : value) {
			set.addAll(s);
		}
		// we have a new set
		sets.add(set);
	}
	
	@Override
	protected void cleanup(Context context) throws IOException, InterruptedException {
		log.info("Number of input sets: " + sets.size());
		log.info("Free heap: " + Runtime.getRuntime().freeMemory()); 
		// compute connected components
		log.info("Computing connected components...");
		List<Set<N>> connectedComponents = computeConnectedComponents(sets);
		// give gc a chance
		sets.clear(); sets = null; System.gc();
		log.info("Number of connected components: " + connectedComponents.size());
		log.info("Free heap: " + Runtime.getRuntime().freeMemory()); 
		// append
		for (Set<N> s : connectedComponents) {
			VALUE.clear();
			
			for (N node : s) {
				VALUE.add(node);
			}
			// N.B. VALUE.disjoint == true is invariant
			context.write(NullWritable.get(), VALUE);
		}
	}
	
	//TODO: refactor this code (it's replicated in PSguidMapBuilderIT)
	public <V extends Object> List<Set<V>> computeConnectedComponents(Collection<? extends Set<V>> input) {
		UndirectedGraph<V, DefaultEdge> graph = 
				new SimpleGraph<V, DefaultEdge>(DefaultEdge.class);
		// construct the initial undirected graph from the given list of sets of connected vertices
		for (Set<V> s : input) {
			// Pseudocode:
			// for 0 <= i < s.size():
			//   generate vertex vi
			// for 1 <= i < s.size();
			//   generate edge {v0,vi}
			Iterator<V> vertices = s.iterator();
			V first = null;

			if (vertices.hasNext()) {
				first = vertices.next();
				graph.addVertex(first);
			}
			while (vertices.hasNext()) {
				V v = vertices.next();
				graph.addVertex(v);
				graph.addEdge(first, v);
			}
		}
		// compute connected components
		ConnectivityInspector<V, DefaultEdge> ci = new ConnectivityInspector<V, DefaultEdge>(graph);
		return ci.connectedSets();
	}
}
