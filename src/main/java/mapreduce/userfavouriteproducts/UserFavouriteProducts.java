package mapreduce.userfavouriteproducts;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class UserFavouriteProducts {


	public static void main(String[] args) throws Exception {

		Job job = new Job(new Configuration(), "UserFavouriteProducts");

		job.setJarByClass(UserFavouriteProducts.class);
		
		job.setMapperClass(UserFavouriteProductsMapper.class);
		job.setReducerClass(UserFavouriteProductsReducer.class);

		
		FileInputFormat.addInputPath(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));

		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(Text.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);

		job.waitForCompletion(true);
	}
}