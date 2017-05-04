package mapreduce_userfavouriteproducts;

import java.io.IOException;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import utils.Product;

public class UserFavouriteProductsReducer extends Reducer<Text, Text, Text, Text>{

	private List<Product> best10products = new LinkedList<>();
	
	@Override
	public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException{
		Map<String, List<Double>> product2scores = new HashMap<>();
		best10products  = new LinkedList<>();

		for (Text value : values){
			String product = value.toString().split("\\|")[0];
			double score = Double.parseDouble(value.toString().split("\\|")[1]);
			if(!product2scores.containsKey(product)){
				List<Double> scores = new LinkedList<>();
				scores.add(score);
				product2scores.put(product, scores);
			}
			else
				product2scores.get(product).add(score);
		}

		for (String productId : product2scores.keySet()) {
			Product product = new Product(productId,averageList(product2scores.get(productId)));
			addValue(product);//productAvg);
		}

		for(Product prodotto : best10products){
			context.write(key, new Text(prodotto.getIdProduct() + "\t" + prodotto.getAverage()));}
	}

	private double averageList(List<Double> scores){
		double sumScores = 0;
		for (Double score : scores)
			sumScores += score;
		return (sumScores / scores.size());
	}

	private void addValue(Product product) {
		if (best10products.size() == 0) {
			best10products.add(product);
		} else if (best10products.get(0).getAverage() > product.getAverage()) {
			best10products.add(0, product);
		} else if (best10products.get(best10products.size() - 1).getAverage() < product.getAverage()) {
			best10products.add(best10products.size(), product);
		} else {
			int i = 0;
			while (best10products.get(i).getAverage() < product.getAverage()) {
				i++;
			}
			best10products.add(i, product);
		}

		if(best10products.size() > 10)
			best10products.remove(0);

	}

}
