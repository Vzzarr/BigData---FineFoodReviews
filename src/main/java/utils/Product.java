package utils;

public class Product {
	private  String idProduct;
	private  double average;
	
	public Product(String idProduct, double average){
		this.idProduct  =idProduct;
		this.average = average;
	}

	public  String getIdProduct() {
		return idProduct;
	}

	public  void setIdProduct(String idProduct) {
		idProduct = idProduct;
	}

	public double getAverage() {
		return average;
	}

	public void setAverage(double average) {
		average = average;
	}
	
	
}
