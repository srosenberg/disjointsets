package foo.bar.disjointsets;

import java.io.IOException;
import java.lang.reflect.Constructor;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;
import org.apache.log4j.Logger;

import foo.bar.utils.HadoopUtils;

/**
 * Implements a distributed algorithm to compute connected components of an undirected graph.
 * The algorithm was adapted from: http://chasebradford.wordpress.com/category/graph-problems/
 * <p>
 * Intuitively, we are given a set of nodes, and we want to compute connected components.
 * E.g., if the input consists of the sets: {a,b}, {b,c}, {d}, then the connected components are: {a,b,c} and {d}.
 * (Note, connected components are necessarily disjoint sets.)
 * <p>
 * The input consists of sequence files.  Each sequence file consists of (totally oredered) sets of nodes abstracted by 
 * <code>NodeSet&lt;N&gt;</code>.  (The actual implementation of <code>Node</code> and <code>NodeSet&lt;N&gt;</code> should be given through configuration
 * options <code>NODE_CLASS</code> and <code>NODESET_CLASS</code>, respectively; see the constructor.)
 * <p>
 * We can define an undirected graph like so: all v | v != fst && v in S ==> {fst, v} in E,
 * where S is an ordered set of nodes, fst is the first element in S and E is the edge relation of the undirected graph.
 * (Note that E is not unique but the connected components are.)
 * Equivalently, the given sets of nodes induce the following "equality" relation: R = {(u,v) | u in S && v in S}.  The goal is then to compute
 * the equivalence closure of R; i.e., the equivalence classes of R are the disjoint sets.   
 * <p>
 * Technicalities.  The algorithm runs, iteratively, a series of election/partition rounds until a fixpoint reached, namely there are no more nondisjoint sets.
 * In election, each intersecting pair of sets is identified by computing the union of their representatives; in subsequent election the intersecting
 * pair is replaced by its union.
 * In partition, we determine if a set is disjoint from all other sets.  Thus, each partition round identifies new connected components.
 * Consequently, when the fixpoint is reached, the algorithm terminates.  The output is the union of all outputs from the partition rounds.
 * For an example using this algorithm, see <code>PSguidMapBuilder</code>.
 * 
 * @author srosenberg
 *
 */
public class ElectionPartition implements MRDefs {
	private Configuration conf;
	
	// configurable properties
	private boolean verbose;
	private int numReducers;
	private boolean optimization;
	private int threshold;
	
	protected Class<Node<?>> nodeClass;
	protected Class<NodeSet<?>> nodeSetClass;
	protected Logger log = Logger.getLogger(ElectionPartition.class);
	
	// intermediate IO paths
	private String parentPath;
	private String electionPath;
	private String partitionPath;
	
	// names
	public final static String VERBOSE = CONF_PREFIX+"verbose";
	public final static String NUM_REDUCERS = CONF_PREFIX+"num_reducers";
	public final static String NODESET_CLASS = CONF_PREFIX+"nodeset_class";
	public final static String NODE_CLASS = CONF_PREFIX+"node_class";
	public final static String OPTIMIZATION = CONF_PREFIX+"optimization";
	public final static String THRESHOLD = CONF_PREFIX+"threshold";
	// defaults
	public final static boolean DEFAULT_VERBOSE = true;
	public final static int DEFAULT_NUM_REDUCERS = 5;
	public final static boolean DEFAULT_OPTIMIZATION = false;
	// minimum number of remaining non-disjoint sets to invoke in-memory computation, provided
	// optimization has been enabled
	public final static int DEFAULT_THRESHOLD = 200000;  
	
	private static final Class<?>[] DUMMY = new Class[]{};
	
	/**
	 * Constructs an instance of Election/Partition algorithm.
	 * 
	 * Configuration parameters in <code>conf</code> will override the defaults.  At the minimum,
	 * <code>NODE_CLASS</code> and <code>NODESET_CLASS</code> should have the fully qualified name of the classes which implement 
	 * <code>Node</code> and <code>NodeSet</code> interfaces.  Furthermore, the jar containing all bundled map/reduce jobs
	 * and their dependencies should have been set using <code>JobConf.setJar</code>.
	 * <p>
	 * precondition: conf != null
	 * 
	 * @param conf	  	 initial configuration that is fed into all jobs
	 * @param parentPath parent directory under which the results of election/partition will be stored
	 */
	public ElectionPartition(JobConf conf, String parentPath) {
		this.conf = conf;
		this.parentPath = parentPath;
		this.electionPath = parentPath + "/election.";
		this.partitionPath = parentPath + "/partition.";
		
		// override defaults from configuration
		this.verbose = conf.getBoolean(VERBOSE, DEFAULT_VERBOSE);
		this.numReducers = conf.getInt(NUM_REDUCERS, DEFAULT_NUM_REDUCERS);
		this.optimization = conf.getBoolean(OPTIMIZATION, DEFAULT_OPTIMIZATION);
		this.threshold = conf.getInt(THRESHOLD, DEFAULT_THRESHOLD);
		init();
	}
	
	protected void init() {
		// init node & nodeSet classes
		nodeClass = getNode(conf);
		nodeSetClass = getNodeSet(conf);
	}
		
	/**
	 * Runs Election/Partition algorithm.
	 * 
	 * @param inputPaths
	 * @return output path(s) containing disjoint sets (may contain globs)
	 * @throws IOException
	 * @throws ClassNotFoundException
	 * @throws InterruptedException
	 */
	public Path run(Path... inputPaths) throws IOException, ClassNotFoundException, InterruptedException {
		int iteration = 0;

		log.info("Starting Election/Partition iterations...");
		
		// set up the chain
		while (true) {
			// election job
			Job election = createElectionJob(iteration);
			// input & output
			if (iteration == 0) {
				// first iteration uses output of preprocessor(s)
				for (Path p : inputPaths) {
					if (p.getFileSystem(conf).exists(p)) {
						FileInputFormat.addInputPath(election, p);
					} else {
						log.warn("Input path: " + p + " does not exist");
					}
				}	
			} else {
				// (paths are chained like so: partition.n => elect.(n+1) =>
				// partition.(n+1) => ...)
				FileInputFormat.setInputPaths(election, new Path(partitionPath
						+ (iteration - 1)));
			}
			FileOutputFormat.setOutputPath(election, new Path(electionPath + iteration));
			
			log.info("Starting Election[" + iteration + "]...");
			submitJob(election, parentPath, "Election failed.");
			long nRemaining = election.getCounters().findCounter(DisjointSetsCounter.NON_DISJOINT).getValue();
			// log total number of remaining non-disjoint sets that were input into ElectionMap 
			log.info("Current number of remaining non-disjoint sets: " + nRemaining);
			
			if (optimization && nRemaining <= threshold) {
				// switch to in-memory computation
				log.info("Reached threshold.  Switching to in-memory computation...");
				Job connectedComponents = createConnectedComponentsJob(null);
				// set input & output paths
				FileInputFormat.setInputPaths(connectedComponents, new Path(electionPath
						+ iteration));
				FileOutputFormat.setOutputPath(connectedComponents, new Path(partitionPath
						+ iteration));
				// submit the job
				submitJob(connectedComponents, parentPath, "Connected components failed.");
				// we're done
				break;
			}
			// partition job
			Job partition = createPartitionJob(iteration);
			// input & output
			FileInputFormat.setInputPaths(partition, new Path(electionPath
					+ iteration));
			FileOutputFormat.setOutputPath(partition, new Path(partitionPath
					+ iteration));
			// TODO: decrease the number of reducers if the number of remaining sets is small?
			log.info("Starting Partition[" + iteration + "]...");
			submitJob(partition, parentPath, "Partition failed.");
			// log total number of disjoint sets which were identified inside PartitionReduce
			log.info("Identified " + partition.getCounters().findCounter(DisjointSetsCounter.DISJOINT).getValue() + 
					" new disjoint sets in Partition[" + iteration + "]");
			// log total number of non-disjoint sets
			nRemaining = partition.getCounters().findCounter(DisjointSetsCounter.NON_DISJOINT).getValue();
			log.info("Current number of remaining non-disjoint sets: " + nRemaining);
			// check if we should continue
			if (nRemaining == 0) {
				// All the sets are disjoint. We're done!
				break;
			}
			// check if optimization should be applied
			if (optimization && nRemaining <= threshold) {
				// switch to in-memory computation
				log.info("Reached threshold.  Switching to in-memory computation...");
				Job connectedComponents = createConnectedComponentsJob(ElectionMap.class);
				// set input & output paths
				FileInputFormat.setInputPaths(connectedComponents, new Path(partitionPath
						+ iteration));
				FileOutputFormat.setOutputPath(connectedComponents, new Path(partitionPath
						+ (iteration + 1)));
				// submit the job
				submitJob(connectedComponents, parentPath, "Connected components failed.");
				// we're done
				break;
			}
			// advance to next iteration
			iteration++;
			log.info("Proceeding with the next Election/Partition iteration: "
					+ iteration);
		}
		log.info("Finished Election/Partition");
		// return all partition outputs
		return getOutputPath();
	}
	
	/**
	 * 
	 * @return globbed path which denotes the output of election/partition 
	 */
	public Path getOutputPath() {
		return new Path(partitionPath + "*");
	}
	
	private Job createElectionJob(int iteration) throws IOException {
		Job election = new Job(conf);
		election.setJobName(getClass().getSimpleName() + "_election_" + iteration);
		// input & output formats
		election.setInputFormatClass(SequenceFileInputFormat.class);
		election.setOutputFormatClass(SequenceFileOutputFormat.class);
	
		// key & value types
		election.setOutputKeyClass(nodeClass);
		election.setOutputValueClass(nodeSetClass);
		// mapper type
		election.setMapperClass(ElectionMap.class);
		// combiner type
		election.setCombinerClass(ElectionReduce.class);
		// reducer type
		election.setReducerClass(ElectionReduce.class);
		// number of reducers
		election.setNumReduceTasks(numReducers);
		
		return election;
	}
	
	private Job createPartitionJob(int iteration) throws IOException {
		Job partition = new Job(conf);
		
		partition.setJobName(getClass().getSimpleName() + "_partition_" + iteration);
		// input & output formats
		partition.setInputFormatClass(SequenceFileInputFormat.class);
		partition.setOutputFormatClass(SequenceFileOutputFormat.class);
		
		// key & value types
		// N.B. map's output key is PSguidOrTid whereas reducer's is
		// NullWritable
		partition.setMapOutputKeyClass(nodeClass);
		partition.setOutputKeyClass(NullWritable.class);
		partition.setOutputValueClass(nodeSetClass);
		// mapper type
		partition.setMapperClass(PartitionMap.class);
		// reducer type
		partition.setReducerClass(PartitionReduce.class);
		// number of reducers
		partition.setNumReduceTasks(numReducers);
		
		return partition;
	}
	
	private Job createConnectedComponentsJob(Class<? extends Mapper> mapperClass) throws IOException {
		Job createConnectedComponents = new Job(conf);
		
		createConnectedComponents.setJobName(getClass().getSimpleName() + "_connected_components");
		// input & output formats
		createConnectedComponents.setInputFormatClass(SequenceFileInputFormat.class);
		createConnectedComponents.setOutputFormatClass(SequenceFileOutputFormat.class);
		// key & value types
		createConnectedComponents.setMapOutputKeyClass(nodeClass);
		createConnectedComponents.setOutputKeyClass(NullWritable.class);
		createConnectedComponents.setOutputValueClass(nodeSetClass);
		// note, that the use identity mapper is used if the mapper class is not set explicitly
		if (mapperClass != null) {
			createConnectedComponents.setMapperClass(mapperClass);
		} 
		// reducer type
		createConnectedComponents.setReducerClass(ConnectedComponentsReduce.class);
		// IMPORTANT: correctness depends on the number of reducers being exactly one!
		createConnectedComponents.setNumReduceTasks(1);
		
		return createConnectedComponents;
	}
	
	void submitJob(Job job, String parentPath, String errMsg) throws IOException, ClassNotFoundException, InterruptedException {
		job.submit();
		// save job.xml
		log.info("Dumping job configs into " + parentPath + "/" + job.getJobName() + ".xml");
		try {
			HadoopUtils.saveJobXml(job.getConfiguration(), parentPath, job.getJobName());
		} catch (IOException ex) {
			log.warn(ex);
		}
		if (verbose) {
			log.info("Tracking URL of " + job.getJobName() + " is " + job.getTrackingURL());
		}
		if (!job.waitForCompletion(verbose)) {
			throw new RuntimeException(errMsg);
		}
	}
	
	// ===================Methods Below Use Reflection to Instantiate Node and NodeSet classes=====================//
	// Note that package access is needed for createXXX methods as these are invoked from mapper/reducer. 
	
	@SuppressWarnings("unchecked")
	static Class<Node<?>> getNode(Configuration conf) {
		try {
			return (Class<Node<?>>) Class.forName(conf.get(NODE_CLASS));
		} catch (Exception ex) {
			throw new RuntimeException("Could not find 'Node' implementation", ex);
		}
	}
	
	@SuppressWarnings("unchecked")
	static Class<NodeSet<?>> getNodeSet(Configuration conf) {
		try {
			return (Class<NodeSet<?>>) Class.forName(conf.get(NODESET_CLASS));
		} catch (Exception ex) {
			throw new RuntimeException("Could not find 'NodeSet' implementation", ex);
		}
	}
	
	@SuppressWarnings("unchecked")
	static <N> Node<N> createNodeInst(Configuration conf) {
		try {
			return (Node<N>) getNode(conf).newInstance();
		} catch (Exception ex) {
			throw new RuntimeException("Could not find 'Node' implementation", ex);
		}
	}
	
	@SuppressWarnings("unchecked")
	static <N extends Node<N>> NodeSet<N> createNodeSetInst(Configuration conf) {
		try {
			Class<NodeSet<?>> nodeSetClass = getNodeSet(conf);
			Constructor<NodeSet<?>> constructor = nodeSetClass.getDeclaredConstructor(DUMMY);
			// just in case it's private
			constructor.setAccessible(true);
			return (NodeSet<N>)constructor.newInstance();
		} catch (Exception ex) {
			throw new RuntimeException("Could not find 'NodeSet' implementation", ex);
		}
	}
}
