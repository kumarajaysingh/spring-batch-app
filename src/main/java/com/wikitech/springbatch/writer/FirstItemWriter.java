package com.wikitech.springbatch.writer;

import java.util.List;

import org.springframework.batch.item.ItemWriter;
import org.springframework.stereotype.Component;

import com.wikitech.springbatch.model.BooksResponse;
import com.wikitech.springbatch.model.StudentJDBC;

@Component
public class FirstItemWriter implements ItemWriter<StudentJDBC> {

	@Override
	public void write(List<? extends StudentJDBC> items) throws Exception {
		// TODO Auto-generated method stub
		System.out.println("Inside Item Writer");
		
		items.stream().forEach(System.out::println);
		
	}

}
