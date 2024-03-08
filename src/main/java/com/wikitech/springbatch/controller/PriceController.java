/*
 * package com.wikitech.springbatch.controller;
 * 
 * import java.util.List;
 * 
 * import org.springframework.beans.factory.annotation.Autowired; import
 * org.springframework.web.bind.annotation.GetMapping; import
 * org.springframework.web.bind.annotation.RequestMapping; import
 * org.springframework.web.bind.annotation.RestController;
 * 
 * import com.wikitech.springbatch.model.BooksResponse; import
 * com.wikitech.springbatch.service.BookService;
 * 
 * @RestController
 * 
 * @RequestMapping("/api") public class PriceController {
 * 
 * @Autowired BookService bookService;
 * 
 * @GetMapping("/call") public String restAPICall() {
 * 
 * List<BooksResponse> responses = bookService.restCallToGetBookPrice();
 * System.out.println(responses.size());
 * 
 * return "return rest api callll"; }
 * 
 * }
 */