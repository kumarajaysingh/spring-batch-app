package com.wikitech.springbatch.processor;

import org.springframework.batch.item.ItemProcessor;
import org.springframework.stereotype.Component;

import com.wikitech.springbatch.model.StudentJDBC;
import com.wikitech.springbatch.model.StudentJSON;

@Component
/*public class FirstItemProcessor implements ItemProcessor<Integer, Long>{//Here Integer is input for itemprocessor and Long is output for itemprocessor

	@Override
	public Long process(Integer item) throws Exception {
		

		System.out.println("Inside First Item processor");
		return Long.valueOf(item+20);
	}*/

public class FirstItemProcessor implements ItemProcessor<StudentJDBC, StudentJSON>{//Here StudentJDBC is input for itemprocessor and StudentJSON is output for itemprocessor

	@Override
	public StudentJSON process(StudentJDBC item) throws Exception {
		

		System.out.println("Inside First Item processor");
		StudentJSON studentJSON = new StudentJSON();
		studentJSON.setId(item.getId());
		studentJSON.setFirstName(item.getFirstName());
		studentJSON.setLastName(item.getLastName());
		studentJSON.setEmail(item.getEmail());
		
		return studentJSON;
	}

}
