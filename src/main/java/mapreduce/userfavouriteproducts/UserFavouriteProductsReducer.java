package mapreduce.userfavouriteproducts;

import java.io.IOException;
import java.util.LinkedList;
import java.util.List;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import utils.Product;

public class UserFavouriteProductsReducer extends Reducer<Text, Text, Text, Text>{

	private List<Product> best10products;
	
	@Override
	public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException{
		best10products = new LinkedList<>();
		
		for (Text value : values){
			String productId = value.toString().split("\\|")[0];
			double score = Double.parseDouble(value.toString().split("\\|")[1]);
			Product product = new Product(productId);
			product.setScore(score);
			addValue(product);
		}
		
		String result = "";

		for(Product prodotto : best10products)
			result += " " + prodotto.getIdProduct() + "|" + prodotto.getScore();
		context.write(key, new Text(result));
	}

	private void addValue(Product product) {
		if (best10products.size() == 0) {
			best10products.add(product);
		} else if (best10products.get(0).getScore() > product.getScore()) {
			best10products.add(0, product);
		} else if (best10products.get(best10products.size() - 1).getScore() < product.getScore()) {
			best10products.add(best10products.size(), product);
		} else {
			int i = 0;
			while (best10products.get(i).getScore() < product.getScore()) {
				i++;
			}
			best10products.add(i, product);
		}
		if(best10products.size() > 10)
			best10products.remove(0);
	}
}