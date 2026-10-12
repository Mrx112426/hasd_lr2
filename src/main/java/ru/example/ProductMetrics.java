package ru.example;

import java.io.Serializable;
import java.math.BigDecimal;

/** Итоговые показатели товара. */
public final class ProductMetrics implements Serializable {
    final String description;
    final long totalQuantity;
    final long ordersWithProduct;
    final BigDecimal grossValue;

    ProductMetrics(
            String description,
            long totalQuantity,
            long ordersWithProduct,
            BigDecimal grossValue) {
        this.description = description;
        this.totalQuantity = totalQuantity;
        this.ordersWithProduct = ordersWithProduct;
        this.grossValue = grossValue;
    }
}
