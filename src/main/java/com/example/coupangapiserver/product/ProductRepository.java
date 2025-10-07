package com.example.coupangapiserver.product;

import com.example.coupangapiserver.product.domain.Product;
import org.springframework.data.jpa.repository.JpaRepository;

public interface ProductRepository extends JpaRepository<Product, Long> {

}
