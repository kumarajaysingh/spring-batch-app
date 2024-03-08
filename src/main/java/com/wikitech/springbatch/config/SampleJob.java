package com.wikitech.springbatch.config;

import java.io.File;
import java.io.IOException;
import java.io.Writer;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.Date;
import java.util.List;

import org.springframework.batch.core.Job;
import org.springframework.batch.core.Step;
import org.springframework.batch.core.configuration.annotation.JobBuilderFactory;
import org.springframework.batch.core.configuration.annotation.StepBuilderFactory;
import org.springframework.batch.core.configuration.annotation.StepScope;
import org.springframework.batch.core.launch.support.RunIdIncrementer;
import org.springframework.batch.item.adapter.ItemReaderAdapter;
import org.springframework.batch.item.adapter.ItemWriterAdapter;
import org.springframework.batch.item.database.BeanPropertyItemSqlParameterSourceProvider;
import org.springframework.batch.item.database.ItemPreparedStatementSetter;
import org.springframework.batch.item.database.JdbcBatchItemWriter;
import org.springframework.batch.item.database.JdbcCursorItemReader;
import org.springframework.batch.item.file.FlatFileFooterCallback;
import org.springframework.batch.item.file.FlatFileHeaderCallback;
import org.springframework.batch.item.file.FlatFileItemReader;
import org.springframework.batch.item.file.FlatFileItemWriter;
import org.springframework.batch.item.file.mapping.BeanWrapperFieldSetMapper;
import org.springframework.batch.item.file.mapping.DefaultLineMapper;
import org.springframework.batch.item.file.transform.BeanWrapperFieldExtractor;
import org.springframework.batch.item.file.transform.DelimitedLineAggregator;
import org.springframework.batch.item.file.transform.DelimitedLineTokenizer;
import org.springframework.batch.item.json.JacksonJsonObjectMarshaller;
import org.springframework.batch.item.json.JacksonJsonObjectReader;
import org.springframework.batch.item.json.JsonFileItemWriter;
import org.springframework.batch.item.json.JsonItemReader;
import org.springframework.batch.item.xml.StaxEventItemReader;
import org.springframework.batch.item.xml.StaxEventItemWriter;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.core.io.FileSystemResource;
import org.springframework.jdbc.core.BeanPropertyRowMapper;
import org.springframework.oxm.jaxb.Jaxb2Marshaller;

import com.wikitech.springbatch.model.BooksResponse;
import com.wikitech.springbatch.model.StudentCSV;
import com.wikitech.springbatch.model.StudentJDBC;
import com.wikitech.springbatch.model.StudentJSON;
import com.wikitech.springbatch.model.StudentXML;
import com.wikitech.springbatch.processor.FirstItemProcessor;
import com.wikitech.springbatch.service.BookService;
//import com.wikitech.springbatch.service.BookService;
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
	
	@Value(value = "${output.flat.file.path}")
	private String outputFlatFilePath;
	
	@Value(value = "${output.flat.file.path1}")
	private String outputFlatFilePath1;

	
	
	  @Autowired
	  DatasourceConfig datasourceConfig;
	 
	
	//for multiple datasource
	
	/*
	 * @Autowired private FirstItemReader firstItemReader;
	 */
	  @Autowired
	  private FirstItemProcessor firstItemProcessor;
	 
	
	@Autowired
	private FirstItemWriter firstItemWriter;
	
	
	  @Autowired BookService bookService;
	 
	
	@Bean
	public Job chunkJob() {
		return jobBuilderFactory.get("chunk job")
				.incrementer(new RunIdIncrementer())
				.start(firstChunkStep())
				.build();
	}

	
	private Step firstChunkStep() {
		return stepBuilderFactory.get("First Chunk Step")
				//.<StudentCSV, StudentCSV>chunk(3)
				.<BooksResponse, BooksResponse>chunk(3)
				//.<StudentJDBC, StudentJSON>chunk(3)//for processor input and output is different model.
				//.reader(flatFileItemReader())
				.reader(flatFileItemReader1())
				//.reader(jsonItemReader())
				//.reader(staxEventItemReader())
				//.reader(jdbcCursorItemReader())
				//.reader(itemReaderAdapter())
			//	.processor(firstItemProcessor)//processor is optional
				//.processor(firstItemProcessor)//processor is optional
				//.writer(firstItemWriter)
				//.writer(flatFileItemWriter())
				//.writer(jsonFileItemWriter(null))
				//.writer(staxEventItemWriter(null))
				//.writer(jdbcBatchItemWriter())
				//.writer(jdbcBatchItemWriter1())
				.writer(itemWriterAdapter())
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
		
		//String query1 = "select bok_id as id, price, offer from mybookprice";
		jdbcCursorItemReader.setDataSource(datasourceConfig.dataSource1());
		jdbcCursorItemReader.setSql(query);
		
		BeanPropertyRowMapper<StudentJDBC> beanPropertyRowMapper = new BeanPropertyRowMapper<StudentJDBC>();
		beanPropertyRowMapper.setMappedClass(StudentJDBC.class);
		
		jdbcCursorItemReader.setRowMapper(beanPropertyRowMapper);
		
		//skipping top 2 record and start reading from 3rd record
	//	jdbcCursorItemReader.setCurrentItemCount(2);
		
	//	jdbcCursorItemReader.setMaxItemCount(8);
		
		return jdbcCursorItemReader;
	}
	
	//reading from rest API
	/*
	 * public ItemReaderAdapter<BooksResponse> itemReaderAdapter() {
	 * 
	 * ItemReaderAdapter<BooksResponse> itemReaderAdapter = new
	 * ItemReaderAdapter<BooksResponse>();
	 * 
	 * //List<BooksResponse> responses = bookService.restCallToGetBookPrice();
	 * 
	 * itemReaderAdapter.setTargetObject(bookService);
	 * itemReaderAdapter.setTargetMethod("getBookPrice");
	 * 
	 * 
	 * 
	 * return itemReaderAdapter;
	 * 
	 * }
	 */
	
	
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
				writer.write("Id|First Name|Last Name|Email");

			}
		});
		flatFileItemWriter.setLineAggregator(new DelimitedLineAggregator<StudentJDBC>() {
			{

				setDelimiter("|");
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
	public JsonFileItemWriter<StudentJSON> jsonFileItemWriter (
			@Value("#{jobParameters[outputFiles]}") FileSystemResource fileSystemResource) {
		JsonFileItemWriter<StudentJSON> jsonFileItemWriter = 
				new JsonFileItemWriter<StudentJSON> (fileSystemResource, new JacksonJsonObjectMarshaller<StudentJSON>());
		
		return jsonFileItemWriter;
	}
	
	//XML Item writer
	@StepScope
	@Bean
	public StaxEventItemWriter<StudentJDBC> staxEventItemWriter (
			@Value("#{jobParameters[outputFiles]}") FileSystemResource fileSystemResource) {
		StaxEventItemWriter<StudentJDBC> staxEventItemWriter =
				new StaxEventItemWriter<StudentJDBC>();
		
		staxEventItemWriter.setResource(fileSystemResource);
		staxEventItemWriter.setRootTagName("students");

		staxEventItemWriter.setMarshaller(new Jaxb2Marshaller() {
			{
				setClassesToBeBound(StudentJDBC.class);
			}
		});
		
		return staxEventItemWriter;
		
	}
	
	//JDBC Item Writer using named parameter
	//reading data from json file and write to database
	@Bean
	public JdbcBatchItemWriter<StudentCSV> jdbcBatchItemWriter () {
		
		JdbcBatchItemWriter<StudentCSV> jdbcBatchItemWriter = 
				new JdbcBatchItemWriter<StudentCSV>();
		
		jdbcBatchItemWriter.setDataSource(datasourceConfig.dataSource1());
		String query = "insert into student(id,first_name,last_name,email) "+
		"values(:id, :firstName, :lastName, :email)";
		
		jdbcBatchItemWriter.setItemSqlParameterSourceProvider(
				new BeanPropertyItemSqlParameterSourceProvider<StudentCSV>());
		jdbcBatchItemWriter.setSql(query);
		
		return jdbcBatchItemWriter;
		
	}
	
	//JDBC Item Writer using prepared statement
		//reading data from json file and write to database
		@Bean
		public JdbcBatchItemWriter<StudentCSV> jdbcBatchItemWriter1 () {
			
			JdbcBatchItemWriter<StudentCSV> jdbcBatchItemWriter = 
					new JdbcBatchItemWriter<StudentCSV>();
			
			jdbcBatchItemWriter.setDataSource(datasourceConfig.dataSource1());
			String query = "insert into student(id,first_name,last_name,email) values(?,?,?,?)";
			jdbcBatchItemWriter.setSql(query);
			jdbcBatchItemWriter.setItemPreparedStatementSetter(
					new ItemPreparedStatementSetter<StudentCSV>() {
						
						@Override
						public void setValues(StudentCSV item, PreparedStatement ps) throws SQLException {
							// TODO Auto-generated method stub
							ps.setLong(1, item.getId());
							ps.setString(2, item.getFirstName());
							ps.setString(3, item.getLastName());
							ps.setString(4, item.getEmail());
						}
					});
			
			return jdbcBatchItemWriter;
			
		}
		
		public FlatFileItemReader<BooksResponse> flatFileItemReader1() {

			FlatFileItemReader<BooksResponse> flatFileItemReader1 = new FlatFileItemReader<BooksResponse>();

			//flatFileItemReader.setResource(new FileSystemResource(new File(flatFile)));//file path hardcoded
			flatFileItemReader1.setResource(new FileSystemResource(new File(outputFlatFilePath1)));
			
			flatFileItemReader1.setLinesToSkip(1);
			
			//alternate way of above code for setting Line Mapper START
			
			DefaultLineMapper<BooksResponse> defaultLineMapper = new DefaultLineMapper<>();
			
			DelimitedLineTokenizer  delimitedLineTokenizer = new DelimitedLineTokenizer();
			delimitedLineTokenizer.setNames("BookID", "Price", "Offer");
			
			defaultLineMapper.setLineTokenizer(delimitedLineTokenizer);
			
			BeanWrapperFieldSetMapper<BooksResponse> fieldSetMapper = new BeanWrapperFieldSetMapper<BooksResponse>();
			fieldSetMapper.setTargetType(BooksResponse.class);
			
			defaultLineMapper.setFieldSetMapper(fieldSetMapper);
			flatFileItemReader1.setLineMapper(defaultLineMapper);
			flatFileItemReader1.setLinesToSkip(1);
			flatFileItemReader1.setMaxItemCount(5);
			//alternate way of above code for setting Line Mapper END
			return flatFileItemReader1;
		}
		
		// item writer for rest API
		public ItemWriterAdapter<BooksResponse> itemWriterAdapter() {

			ItemWriterAdapter<BooksResponse> itemWriterAdapter = new ItemWriterAdapter<BooksResponse>();

			// List<BooksResponse> responses = bookService.restCallToGetBookPrice();

			itemWriterAdapter.setTargetObject(bookService);
			itemWriterAdapter.setTargetMethod("restCallToCreateBookPrice");

			return itemWriterAdapter;

		}
	
}
