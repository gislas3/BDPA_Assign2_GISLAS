package PreProc;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

public class PreProcess extends Configured implements Tool {

	static { Configuration.addDefaultResource("hdfs-default.xml"); 
	Configuration.addDefaultResource("hdfs-site.xml"); 
	Configuration.addDefaultResource("yarn-default.xml"); 
	Configuration.addDefaultResource("yarn-site.xml"); 
	Configuration.addDefaultResource("mapred-default.xml"); 
	Configuration.addDefaultResource("mapred-site.xml");
	}
	public static void main(String[] args) throws Exception {
		// TODO Auto-generated method stub
		int status = ToolRunner.run(new PreProcess(), args);
		System.exit(status);
	}
	@Override
	public int run(String args[]) throws Exception{
		
		return 0;
	}

}
