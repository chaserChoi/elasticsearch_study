package com.example.coupangapiserver.product;

import com.example.coupangapiserver.product.domain.Product;
import com.example.coupangapiserver.product.dto.CreateProductRequestDto;
import java.util.List;
import org.springframework.data.domain.PageRequest;
import org.springframework.data.domain.Pageable;
import org.springframework.stereotype.Service;

@Service
public class ProductService {

  private final ProductRepository productRepository;

  public ProductService(ProductRepository productRepository) {
    this.productRepository = productRepository;
  }

  public List<Product> getProducts(int page, int size) {
    Pageable pageable = PageRequest.of(page - 1, size);
    return productRepository.findAll(pageable).getContent();
  }

  public Product createProduct(CreateProductRequestDto createProductRequestDto) {
    Product product = new Product(
        createProductRequestDto.getName(),
        createProductRequestDto.getDescription(),
        createProductRequestDto.getPrice(),
        createProductRequestDto.getRating(),
        createProductRequestDto.getCategory()
    );
    return productRepository.save(product);
  }

  public void deleteProduct(Long id) {
    productRepository.deleteById(id);
  }
}
