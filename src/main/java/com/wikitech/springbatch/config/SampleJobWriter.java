package com.wikitech.springbatch.config;

import java.io.File;
import java.io.IOException;
import java.io.Writer;
import java.util.Date;

import org.springframework.batch.core.Job;
import org.springframework.batch.core.Step;
import org.springframework.batch.core.configuration.annotation.JobBuilderFactory;
import org.springframework.batch.core.configuration.annotation.StepBuilderFactory;
import org.springframework.batch.core.configuration.annotation.StepScope;
import org.springframework.batch.core.launch.support.RunIdIncrementer;
import org.springframework.batch.item.database.JdbcCursorItemReader;
import org.springframework.batch.item.file.FlatFileFooterCallback;
import org.springframework.batch.item.file.FlatFileHeaderCallback;
import org.springframework.batch.item.file.FlatFileItemWriter;
import org.springframework.batch.item.file.transform.BeanWrapperFieldExtractor;
import org.springframework.batch.item.file.transform.DelimitedLineAggregator;
import org.springframework.batch.item.json.JacksonJsonObjectMarshaller;
import org.springframework.batch.item.json.JsonFileItemWriter;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.core.io.FileSystemResource;
import org.springframework.jdbc.core.BeanPropertyRowMapper;

import com.wikitech.springbatch.model.BooksResponse;
import com.wikitech.springbatch.model.StudentJDBC;
//import com.wikitech.springbatch.service.BookService;
import com.wikitech.springbatch.writer.FirstItemWriter;

//@Configuration
public class SampleJobWriter {

	@Autowired
	private JobBuilderFactory jobBuilderFactory;

	@Autowired
	private StepBuilderFactory stepBuilderFactory;

	@Value(value = "${output.flat.file.path}")
	private String outputFlatFilePath;
	
	@Value(value = "${output.flat.file.path1}")
	private String outputFlatFilePath1;

	@Autowired
	DatasourceConfig datasourceConfig;

	/*
	 * @Autowired BookService bookService;
	 */

	@Bean
	public Job chunkJob() {
		return jobBuilderFactory.get("chunk job").incrementer(new RunIdIncrementer()).start(firstChunkStep()).build();
	}

	private Step firstChunkStep() {
		return stepBuilderFactory.get("First Chunk Step")
				//.<StudentJDBC, StudentJDBC>chunk(3)
				.<BooksResponse, BooksResponse>chunk(3)
				// .reader(flatFileItemReader())
				// .reader(jsonItemReader())
				// .reader(staxEventItemReader())
				//.reader(jdbcCursorItemReader())
				.reader(jdbcCursorItemReader1())
				// .reader(itemReaderAdapter())

				// .processor(firstItemProcessor)//processor is optional
			//	.writer(firstItemWriter)
				//.writer(flatFileItemWriter())
				//.writer(jsonFileItemWriter(null))
				.writer(flatFileItemWriter1())
				.build();
	}
	
	public JdbcCursorItemReader<StudentJDBC> jdbcCursorItemReader() {
		JdbcCursorItemReader<StudentJDBC> jdbcCursorItemReader = new JdbcCursorItemReader<StudentJDBC>();
		String query = "select id, first_name as firstName, last_name as lastName, email from student";
		
		String query1 = "select bok_id as id, price, offer from mybookprice";
		jdbcCursorItemReader.setDataSource(datasourceConfig.dataSource1());
		jdbcCursorItemReader.setSql(query);
		
		BeanPropertyRowMapper<StudentJDBC> beanPropertyRowMapper = new BeanPropertyRowMapper<StudentJDBC>();
		beanPropertyRowMapper.setMappedClass(StudentJDBC.class);
		
		jdbcCursorItemReader.setRowMapper(beanPropertyRowMapper);
		
		//skipping top 2 record and start reading from 3rd record
		jdbcCursorItemReader.setCurrentItemCount(2);
		
		jdbcCursorItemReader.setMaxItemCount(8);
		
		return jdbcCursorItemReader;
	}

	// FOR CSV file
	@Bean
	public FlatFileItemWriter<StudentJDBC> flatFileItemWriter() {

		// FlatFileItemWriter<StudentJDBC> flatFileItemWriter = new
		// FlatFileItemWriter<StudentJDBC>();
		// Java17 var feature
		var flatFileItemWriter = new FlatFileItemWriter<StudentJDBC>();

		flatFileItemWriter.setResource(new FileSystemResource(new File(outputFlatFilePath)));

		flatFileItemWriter.setHeaderCallback(new FlatFileHeaderCallback() {

			@Override
			public void writeHeader(Writer writer) throws IOException {
				// TODO Auto-generated method stub
				writer.write("Id,First Name,Last Name,Email");

			}
		});
		flatFileItemWriter.setLineAggregator(new DelimitedLineAggregator<StudentJDBC>() {
			{

				setFieldExtractor(new BeanWrapperFieldExtractor<StudentJDBC>() {
					{
						setNames(new String[] { "id", "firstName", "lastName", "email" });
					}
				});
			}
		});
		
		flatFileItemWriter.setFooterCallback(new FlatFileFooterCallback() {
			
			@Override
			public void writeFooter(Writer writer) throws IOException {
				// TODO Auto-generated method stub
				writer.write("Created @ "+ new Date());
				
			}
		});

		return flatFileItemWriter;

	}
	@StepScope
	@Bean
	public JsonFileItemWriter<StudentJDBC> jsonFileItemWriter (
			@Value("#{jobParameters[outputFiles]}") FileSystemResource fileSystemResource) {
		JsonFileItemWriter<StudentJDBC> jsonFileItemWriter = 
				new JsonFileItemWriter<StudentJDBC> (fileSystemResource, new JacksonJsonObjectMarshaller<StudentJDBC>());
		
		return jsonFileItemWriter;
	}
	
	//for bookprice databases
	//START
	
	public JdbcCursorItemReader<BooksResponse> jdbcCursorItemReader1() {
		JdbcCursorItemReader<BooksResponse> jdbcCursorItemReader1 = new JdbcCursorItemReader<BooksResponse>();
		String query = "select book_id, price, offer from mybookprice";
		
		//String query1 = "select bok_id as id, price, offer from mybookprice";
		jdbcCursorItemReader1.setDataSource(datasourceConfig.rccDataSource());
		jdbcCursorItemReader1.setSql(query);
		
		BeanPropertyRowMapper<BooksResponse> beanPropertyRowMapper = new BeanPropertyRowMapper<BooksResponse>();
		beanPropertyRowMapper.setMappedClass(BooksResponse.class);
		
		jdbcCursorItemReader1.setRowMapper(beanPropertyRowMapper);
		
		//skipping top 2 record and start reading from 3rd record
	//	jdbcCursorItemReader.setCurrentItemCount(2);
		
		//jdbcCursorItemReader.setMaxItemCount(8);
		
		return jdbcCursorItemReader1;
	}

	// FOR CSV file
	@Bean
	public FlatFileItemWriter<BooksResponse> flatFileItemWriter1() {

		// FlatFileItemWriter<StudentJDBC> flatFileItemWriter = new
		// FlatFileItemWriter<StudentJDBC>();
		// Java17 var feature
		var flatFileItemWriter = new FlatFileItemWriter<BooksResponse>();

		flatFileItemWriter.setResource(new FileSystemResource(new File(outputFlatFilePath1)));

		flatFileItemWriter.setHeaderCallback(new FlatFileHeaderCallback() {

			@Override
			public void writeHeader(Writer writer) throws IOException {
				// TODO Auto-generated method stub
				writer.write("BookId,Price,Offer");

			}
		});
		flatFileItemWriter.setLineAggregator(new DelimitedLineAggregator<BooksResponse>() {
			{

				setFieldExtractor(new BeanWrapperFieldExtractor<BooksResponse>() {
					{
						setNames(new String[] { "book_id", "price", "offer"});
					}
				});
			}
		});
		
		flatFileItemWriter.setFooterCallback(new FlatFileFooterCallback() {
			
			@Override
			public void writeFooter(Writer writer) throws IOException {
				// TODO Auto-generated method stub
				writer.write("Created @ "+ new Date());
				
			}
		});

		return flatFileItemWriter;

	}
	
	//END

}
