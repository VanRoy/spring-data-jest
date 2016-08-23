package com.github.vanroy.springboot.autoconfigure.data.jest.repositories;

import java.util.List;

import com.github.vanroy.springboot.autoconfigure.data.jest.entities.Product;
import org.springframework.data.repository.PagingAndSortingRepository;

/**
 * Created by akonczak on 04/09/15.
 */
public interface ProductRepository extends PagingAndSortingRepository<Product, String> {

	List<Product> findByNameAndText(String name, String text);

	List<Product> findByNameAndPrice(String name, Float price);

}
