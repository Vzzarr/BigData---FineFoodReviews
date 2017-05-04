package mapreduce_bestproductmonth;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class BestProductMonth {

	public static void main(String[] args) throws Exception {

		Job job = new Job(new Configuration(), "BestProductMonth");

		job.setJarByClass(BestProductMonth.class);
		
		job.setMapperClass(BestProductMonthMapper.class);
		job.setReducerClass(BestProductMonthReducer.class);

		
		FileInputFormat.addInputPath(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));

		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(Text.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);
		
		job.setNumReduceTasks(1);

		job.waitForCompletion(true);
	}
}