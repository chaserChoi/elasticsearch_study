package com.example.coupangapiserver.product;

import com.example.coupangapiserver.product.domain.Product;
import com.example.coupangapiserver.product.domain.ProductDocument;
import com.example.coupangapiserver.product.dto.CreateProductRequestDto;
import java.util.List;
import org.springframework.data.domain.PageRequest;
import org.springframework.data.domain.Pageable;
import org.springframework.stereotype.Service;

@Service
public class ProductService {

  private final ProductRepository productRepository;
  private final ProductDocumentRepository productDocumentRepository;

  public ProductService(ProductRepository productRepository, ProductDocumentRepository productDocumentRepository) {
    this.productRepository = productRepository;
    this.productDocumentRepository = productDocumentRepository;
  }

  public List<Product> getProducts(int page, int size) {
    Pageable pageable = PageRequest.of(page - 1, size);
    return productRepository.findAll(pageable).getContent();
  }

  // 상품 추가 로직
  public Product createProduct(CreateProductRequestDto createProductRequestDto) {
    Product product = new Product(
        createProductRequestDto.getName(),
        createProductRequestDto.getDescription(),
        createProductRequestDto.getPrice(),
        createProductRequestDto.getRating(),
        createProductRequestDto.getCategory()
    );

    // 수정
    Product savedProduct = productRepository.save(product);

    // 여기서부터 추가
    ProductDocument productDocument = new ProductDocument(
          savedProduct.getId().toString(),
          savedProduct.getName(),
          savedProduct.getDescription(),
          savedProduct.getPrice(),
          savedProduct.getRating(),
          savedProduct.getCategory()
    );

    productDocumentRepository.save(productDocument);

    return savedProduct;
  }

  // 상품 삭제 로직
  public void deleteProduct(Long id) {
    productRepository.deleteById(id);

    productDocumentRepository.deleteById(id.toString()); // 추가
  }
}
