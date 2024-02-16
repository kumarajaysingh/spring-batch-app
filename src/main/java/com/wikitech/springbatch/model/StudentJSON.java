package com.wikitech.springbatch.model;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonProperty;

//if we want to process partial fields then we have to add @JsonIgnoredProperties annotation
//to the class level. for example in this class we dont want last name but last name is 
//available in json file. if we enabled lastName property, then @JsonIgnoredPropertis is not required

@JsonIgnoreProperties(ignoreUnknown = true)
public class StudentJSON {

	private Long id;

	//in case of property miss match bitween model class and json file, use @JsonProperty
	@JsonProperty("first_name")
	private String firstName;

	//private String lastName;

	private String email;

	public Long getId() {
		return id;
	}

	public void setId(Long id) {
		this.id = id;
	}

	public String getFirstName() {
		return firstName;
	}

	public void setFirstName(String firstName) {
		this.firstName = firstName;
	}
	/*
	 * public String getLastName() { return lastName; }
	 * 
	 * public void setLastName(String lastName) { this.lastName = lastName; }
	 */

	public String getEmail() {
		return email;
	}

	public void setEmail(String email) {
		this.email = email;
	}

	@Override
	public String toString() {
		return "StudentJSON [id=" + id + ", firstName=" + firstName
				+ /* ", lastName=" + lastName + */", email=" + email
				+ "]";
	}
	
	

}
