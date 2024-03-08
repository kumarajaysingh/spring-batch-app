
package com.wikitech.springbatch.service;

import java.util.ArrayList;
import java.util.List;

import org.springframework.http.ResponseEntity;
import org.springframework.stereotype.Service;
import org.springframework.web.client.RestTemplate;

import com.wikitech.springbatch.model.BooksResponse;

@Service
public class BookService {

	List<BooksResponse> bookList = null;

	public List<BooksResponse> restCallToGetBookPrice() {

		String url = "http://localhost:9090/bookprice";
		bookList = new ArrayList<>();

		RestTemplate restTemplate = new RestTemplate();

		BooksResponse[] booksResponses1 = restTemplate.getForObject(url, BooksResponse[].class);

		ResponseEntity<BooksResponse[]> booksResponses = restTemplate.getForEntity(url, BooksResponse[].class);

		// BooksResponse[] list = booksResponses.getBody();
		for (BooksResponse book : booksResponses1) {
			bookList.add(book);
		}

		return bookList;
	}

	public BooksResponse getBookPrice() {

		if (bookList == null) {
			restCallToGetBookPrice();
		}

		if (bookList != null && !bookList.isEmpty()) {
			return bookList.remove(0);

		}
		return null;
	}
	
	public BooksResponse restCallToCreateBookPrice(BooksResponse booksResponse) {

		String url = "http://localhost:9090/bookprice";
		bookList = new ArrayList<>();

		RestTemplate restTemplate = new RestTemplate();


		return restTemplate.postForObject(url, booksResponse, BooksResponse.class);

	}

	
	

}
