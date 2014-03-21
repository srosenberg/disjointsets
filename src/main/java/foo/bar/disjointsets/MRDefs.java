package foo.bar.disjointsets;

public interface MRDefs {
	// used to prefix configuration properties
	String CONF_PREFIX = "election_partition.";
	
	// used primarily by Election/Reduce MR Jobs
	enum DisjointSetsCounter { 
		NON_DISJOINT, DISJOINT, 
	}
	
	enum MiscCounters {
		HEARTBEAT
	}
}
