package foo.bar.utils;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.Counter;
import org.apache.hadoop.mapreduce.TaskInputOutputContext;

public class HadoopUtils {

	public static void saveJobXml(Configuration conf, String dir, String jobName) throws IOException {
		Path path = new Path(dir, jobName + ".xml");
		FSDataOutputStream out = null;
		
		try {
			out = path.getFileSystem(conf).create(path);
			conf.writeXml(out);
		} finally {
			try {
				if (out != null) out.close();
			} catch (Exception ex) {
				ex.printStackTrace();
			}
		}
	}
	
	/**
	 * 
	 * Converts <code>counterName</code> to a canonical representation wherein the group of the counter
	 * is not the fully-qualified class name but rather a "simple" name as per <code>Class.getSimpleName()</code>.
	 * 
	 * @param context
	 * @param counterName
	 * @return
	 */
	public static Counter getCanonicalCounter(TaskInputOutputContext<?, ?, ?, ?> context, Enum<?> counterName) {
		return context.getCounter(counterName.getClass().getSimpleName(), counterName.toString());
	}
	
	public static Counter getCanonicalCounter(TaskInputOutputContext<?, ?, ?, ?> context, Class<?> group, String name) {
		return context.getCounter(group.getSimpleName(), name);
	}
}
