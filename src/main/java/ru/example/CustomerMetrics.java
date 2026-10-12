package ru.example;

import java.io.Serializable;
import java.math.BigDecimal;

/** Итоговые показатели клиента. */
public final class CustomerMetrics implements Serializable {
    final long ordersCount;
    final BigDecimal totalSpent;

    CustomerMetrics(long ordersCount, BigDecimal totalSpent) {
        this.ordersCount = ordersCount;
        this.totalSpent = totalSpent;
    }

    CustomerMetrics add(CustomerMetrics other) {
        return new CustomerMetrics(
                ordersCount + other.ordersCount, totalSpent.add(other.totalSpent)
        );
    }

    BigDecimal averageOrderValue() {
        if (ordersCount == 0) {
            return null;
        }
        return totalSpent.divide(BigDecimal.valueOf(ordersCount), 2,
                java.math.RoundingMode.HALF_UP);
    }
}
