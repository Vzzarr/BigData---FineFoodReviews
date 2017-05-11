package mapreduce.userrelateds;

import java.net.URI;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

public class UserRelateds extends Configured implements Tool {
	public static void main(String[] args) throws Exception {
		double startTime = System.currentTimeMillis();
		int res = ToolRunner.run(new Configuration(), new UserRelateds(), args);
		double stopTime = System.currentTimeMillis();
		double elapsedTime = (stopTime - startTime) / 1000;
		System.out.println("TEMPO DI ESECUZIONE:\t" + elapsedTime + "s");
		Configuration conf = new Configuration();
		FileSystem  hdfs = FileSystem.get(URI.create("hdfs://localhost:9000"), conf);
		Path path = new Path("/output/temp.txt");
		if(hdfs.exists(path))
			hdfs.delete(path, true);
		System.exit(res);
	}

	public int run(String[] args) throws Exception {
		Configuration conf = this.getConf();
		Job job1 = new Job(conf, "User Relateds Pt1");
		Path temp1 = new Path("/output/temp.txt");
		Path input = new Path(args[0]);
		Path output = new Path(args[1]);

		job1.setJarByClass(UserRelateds.class);

		job1.setMapperClass(UserRelatedsMapper.class);
		job1.setReducerClass(UserRelatedsReducer.class);

		FileInputFormat.addInputPath(job1, input);
		FileOutputFormat.setOutputPath(job1, temp1);

		job1.setMapOutputKeyClass(Text.class);
		job1.setMapOutputValueClass(Text.class);
		job1.setOutputKeyClass(Text.class);
		job1.setOutputValueClass(Text.class);


		job1.waitForCompletion(true);

		Job job2 = new Job(conf, "User Relateds Pt2");
		job2.setJarByClass(UserRelateds.class);

		job2.setMapperClass(UserRelatedsMapper1.class);
		job2.setReducerClass(UserRelatedsReducer1.class);

		job2.setMapOutputKeyClass(Text.class);
		job2.setMapOutputValueClass(Text.class);
		job2.setOutputKeyClass(Text.class);
		job2.setOutputValueClass(Text.class);


		FileInputFormat.setInputPaths(job2, temp1);
		FileOutputFormat.setOutputPath(job2, output);

		return job2.waitForCompletion(true) ? 0 : 1;
	}
}
