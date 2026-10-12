package ru.example;

import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.storage.StorageLevel;
import scala.Tuple2;

import java.math.BigDecimal;

/** Построение витрины заказов и клиентских метрик. */
public final class CustomerAnalytics {
    private CustomerAnalytics() {
    }

    public static Result calculate(
            JavaRDD<Transaction> cleanRows,
            JavaRDD<Transaction> sales) {
        JavaPairRDD<String, BigDecimal> orders = sales
                .filter(row -> !row.customerId.isEmpty())
                .mapToPair(row -> new Tuple2<>(
                        row.customerId + "\u0000" + row.invoiceNo,
                        row.lineTotal()
                ))
                .reduceByKey(BigDecimal::add)
                .persist(StorageLevel.MEMORY_AND_DISK());

        // Один элемент orders — один заказ, поэтому начинаем счётчик с 1.
        JavaPairRDD<String, CustomerMetrics> calculated = orders
                .mapToPair(pair -> new Tuple2<>(
                        pair._1.split("\u0000", 2)[0],
                        new CustomerMetrics(1, pair._2)
                ))
                .reduceByKey(CustomerMetrics::add);

        JavaPairRDD<String, Integer> allCustomers = cleanRows
                .filter(row -> !row.customerId.isEmpty())
                .map(row -> row.customerId)
                .distinct()
                .mapToPair(id -> new Tuple2<>(id, 1));

        JavaPairRDD<String, CustomerMetrics> customerMetrics = allCustomers
                .leftOuterJoin(calculated)
                .mapValues(value -> value._2.orElse(new CustomerMetrics(0, BigDecimal.ZERO)))
                .persist(StorageLevel.MEMORY_AND_DISK());

        return new Result(orders, customerMetrics);
    }

    public static final class Result {
        final JavaPairRDD<String, BigDecimal> orders;
        final JavaPairRDD<String, CustomerMetrics> customerMetrics;

        Result(
                JavaPairRDD<String, BigDecimal> orders,
                JavaPairRDD<String, CustomerMetrics> customerMetrics) {
            this.orders = orders;
            this.customerMetrics = customerMetrics;
        }
    }
}
