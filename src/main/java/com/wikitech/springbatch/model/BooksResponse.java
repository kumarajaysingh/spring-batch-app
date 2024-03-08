package com.wikitech.springbatch.model;

public class BooksJDBC {

	private Long id;

	private String price;

	private String offer;

	public Long getId() {
		return id;
	}

	public void setId(Long id) {
		this.id = id;
	}

	public String getPrice() {
		return price;
	}

	public void setPrice(String price) {
		this.price = price;
	}

	public String getOffer() {
		return offer;
	}

	public void setOffer(String offer) {
		this.offer = offer;
	}

	@Override
	public String toString() {
		return "BooksJDBC [id=" + id + ", price=" + price + ", offer=" + offer + "]";
	}
	
	

	}
