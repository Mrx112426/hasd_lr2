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

    BigDecimal averageOrderValue() {
        if (ordersCount == 0) {
            return null;
        }
        return totalSpent.divide(BigDecimal.valueOf(ordersCount), 8,
                java.math.RoundingMode.HALF_UP);
    }
}
