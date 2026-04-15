package com.juliodev.ProductsMicroservice.service;

import com.juliodev.ProductsMicroservice.rest.CreateProductRestModel;

public interface ProductService {

    String createProduct(CreateProductRestModel productRestModel) throws Exception;
}
