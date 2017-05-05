package utils;

public class Product {
	private  String idProduct;
	private  double average;
	private double score;
	
	public Product(String idProduct){
		this.idProduct  =idProduct;
	}

	public double getScore() {
		return score;
	}

	public void setScore(double score) {
		this.score = score;
	}

	public  String getIdProduct() {
		return idProduct;
	}

	public  void setIdProduct(String idProduct) {
		this.idProduct = idProduct;
	}

	public double getAverage() {
		return average;
	}

	public void setAverage(double average) {
		this.average = average;
	}
	
	
}