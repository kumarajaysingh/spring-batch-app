package com.wikitech.springbatch.config;

import java.io.File;

import org.springframework.batch.core.Job;
import org.springframework.batch.core.Step;
import org.springframework.batch.core.configuration.annotation.JobBuilderFactory;
import org.springframework.batch.core.configuration.annotation.StepBuilderFactory;
import org.springframework.batch.core.launch.support.RunIdIncrementer;
import org.springframework.batch.item.database.JdbcCursorItemReader;
import org.springframework.batch.item.file.FlatFileItemReader;
import org.springframework.batch.item.file.mapping.BeanWrapperFieldSetMapper;
import org.springframework.batch.item.file.mapping.DefaultLineMapper;
import org.springframework.batch.item.file.transform.DelimitedLineTokenizer;
import org.springframework.batch.item.json.JacksonJsonObjectReader;
import org.springframework.batch.item.json.JsonItemReader;
import org.springframework.batch.item.xml.StaxEventItemReader;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.core.io.FileSystemResource;
import org.springframework.jdbc.core.BeanPropertyRowMapper;
import org.springframework.oxm.jaxb.Jaxb2Marshaller;

import com.wikitech.springbatch.model.BooksJDBC;
import com.wikitech.springbatch.model.StudentCSV;
import com.wikitech.springbatch.model.StudentJDBC;
import com.wikitech.springbatch.model.StudentJSON;
import com.wikitech.springbatch.model.StudentXML;
import com.wikitech.springbatch.writer.FirstItemWriter;

@Configuration
public class SampleJob {
	
	@Autowired
	private JobBuilderFactory jobBuilderFactory;

	@Autowired
	private StepBuilderFactory stepBuilderFactory;
	
	@Value(value = "${flat.file.path}")
	private String flatFilePath;
	
	@Value(value = "${json.file.path}")
	private String jsonFilePath;
	
	@Value(value = "${xml.file.path}")
	private String xmlFilePath;
	
	
	  @Autowired
	  DatasourceConfig datasourceConfig;
	 
	
	//for multiple datasource
	
	/*
	 * @Autowired private FirstItemReader firstItemReader;
	 * 
	 * @Autowired private FirstItemProcessor firstItemProcessor;
	 */
	
	@Autowired
	private FirstItemWriter firstItemWriter;
	
	@Bean
	public Job chunkJob() {
		return jobBuilderFactory.get("chunk job")
				.incrementer(new RunIdIncrementer())
				.start(firstChunkStep())
				.build();
	}

	
	private Step firstChunkStep() {
		return stepBuilderFactory.get("First Chunk Step")
				.<StudentJDBC, StudentJDBC>chunk(3)
				//.reader(flatFileItemReader())
				//.reader(jsonItemReader())
				//.reader(staxEventItemReader())
				.reader(jdbcCursorItemReader())
				
				
				//.processor(firstItemProcessor)//processor is optional
				.writer(firstItemWriter)
				.build();
	}
	
	/*
	 * @StepScope
	 * 
	 * @Bean public FlatFileItemReader<StudentCSV>
	 * flatFileItemReader(@Value("#{jobParameters['inputFile']}") FileSystemResource
	 * fileSystemResource) {
	 */
	
	//FOR CSV file
	public FlatFileItemReader<StudentCSV> flatFileItemReader() {

		//String flatFile = "F:\\2024_Work\\eclipse-workspace\\projects\\spring-batch-app\\InputFiles\\students.csv";
		FlatFileItemReader<StudentCSV> flatFileItemReader = new FlatFileItemReader<StudentCSV>();

		//flatFileItemReader.setResource(new FileSystemResource(new File(flatFile)));//file path hardcoded
		flatFileItemReader.setResource(new FileSystemResource(new File(flatFilePath)));
		//flatFileItemReader.setResource(fileSystemResource);

		/*
		flatFileItemReader.setLineMapper(new DefaultLineMapper<StudentCSV>() {

			{
				setLineTokenizer(new DelimitedLineTokenizer() {
					{
						setNames("ID", "First Name", "Last Name", "Email");
						//setDelimiter("|");//if we want to read file with pipe(|) seperator instead of commas(')
						//for example:  1|Dud|Ishak|dishak0@alexa.com
					}
				});
				
				setFieldSetMapper(new BeanWrapperFieldSetMapper<StudentCSV>() {
					{
						setTargetType(StudentCSV.class);
					}
				});
			}
		});
		*/
		flatFileItemReader.setLinesToSkip(1);
		
		//alternate way of above code for setting Line Mapper START
		
		DefaultLineMapper<StudentCSV> defaultLineMapper = new DefaultLineMapper<>();
		
		DelimitedLineTokenizer  delimitedLineTokenizer = new DelimitedLineTokenizer();
		delimitedLineTokenizer.setNames("ID", "First Name", "Last Name", "Email");
		
		defaultLineMapper.setLineTokenizer(delimitedLineTokenizer);
		
		BeanWrapperFieldSetMapper<StudentCSV> fieldSetMapper = new BeanWrapperFieldSetMapper<StudentCSV>();
		fieldSetMapper.setTargetType(StudentCSV.class);
		
		defaultLineMapper.setFieldSetMapper(fieldSetMapper);
		flatFileItemReader.setLineMapper(defaultLineMapper);
		flatFileItemReader.setLinesToSkip(1);
		//alternate way of above code for setting Line Mapper END
		return flatFileItemReader;
	}
	
	//For JSON file
	public JsonItemReader<StudentJSON> jsonItemReader() {
		
		JsonItemReader<StudentJSON> jsonItemReader = new JsonItemReader<StudentJSON>();
		jsonItemReader.setResource(new FileSystemResource(new File(jsonFilePath)));
		jsonItemReader.setJsonObjectReader(new JacksonJsonObjectReader<>(StudentJSON.class));
		
		//if we want to read only top 4 json record from json file
		jsonItemReader.setMaxItemCount(4);
		
		//if we want to skip few record from top.
		jsonItemReader.setCurrentItemCount(2);//it will start reading from 2nd record
		
		return jsonItemReader;
	}
	
	//For XML file
	public StaxEventItemReader<StudentXML> staxEventItemReader () {
		
		StaxEventItemReader< StudentXML> staxEventItemReader = new StaxEventItemReader<StudentXML>();
		
		staxEventItemReader.setResource(new FileSystemResource(new File(xmlFilePath)));
		staxEventItemReader.setFragmentRootElementName("student");
		staxEventItemReader.setUnmarshaller(new Jaxb2Marshaller() {
			{
			setClassesToBeBound(StudentXML.class);
			}
		});
		
		return staxEventItemReader;
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
	
}
