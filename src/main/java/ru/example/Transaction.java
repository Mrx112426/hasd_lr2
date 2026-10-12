package ru.example;

import java.io.Serializable;
import java.math.BigDecimal;
import java.util.List;

/** Очищенная и типизированная строка исходного датасета. */
public final class Transaction implements Serializable {
    final String invoiceNo;
    final String stockCode;
    final String description;
    final long quantity;
    final String invoiceDate;
    final BigDecimal unitPrice;
    final String customerId;
    final String country;

    private Transaction(
            String invoiceNo,
            String stockCode,
            String description,
            long quantity,
            String invoiceDate,
            BigDecimal unitPrice,
            String customerId,
            String country) {
        this.invoiceNo = invoiceNo;
        this.stockCode = stockCode;
        this.description = description;
        this.quantity = quantity;
        this.invoiceDate = invoiceDate;
        this.unitPrice = unitPrice;
        this.customerId = customerId;
        this.country = country;
    }

    public static Transaction fromRow(List<String> row) {
        if (row.size() != 8) {
            throw new IllegalArgumentException(
                    "Ожидалось 8 колонок, получено: " + row.size()
            );
        }

        try {
            return new Transaction(
                    row.get(0).trim(),
                    row.get(1).trim(),
                    row.get(2).trim(),
                    Long.parseLong(row.get(3).trim()),
                    row.get(4).trim(),
                    new BigDecimal(row.get(5).trim()),
                    row.get(6).trim(),
                    row.get(7).trim()
            );
        } catch (NumberFormatException exception) {
            throw new IllegalArgumentException("Ошибка числового поля: " + row, exception);
        }
    }

    BigDecimal lineTotal() {
        return unitPrice.multiply(BigDecimal.valueOf(quantity));
    }
}
