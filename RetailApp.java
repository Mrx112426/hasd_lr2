package ru.example;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.Optional;
import scala.Tuple2;

import java.io.Serializable;
import java.math.BigDecimal;
import java.math.RoundingMode;
import java.nio.charset.Charset;
import java.util.*;

/**
 * Online Retail: два топ-5 товаров и метрики всех известных клиентов.
 * Аргумент: путь к исходному data.csv в кодировке Windows-1252.
 * Результаты выводятся в консоль. Возвраты не вычитаются: расчёт gross.
 */
public class RetailApp {

    private static final Set<String> SERVICE_CODES = new HashSet<>(
            Arrays.asList(
                    "POST", "DOT", "C2", "M", "D",
                    "BANK CHARGES", "AMAZONFEE", "CRUK", "B", "S"
            )
    );

    public static void main(String[] args) {
        if (args.length == 0) {
            System.err.println("Использование: RetailApp <путь к data.csv>");
            return;
        }

        SparkConf conf = new SparkConf()
                .setAppName("Online Retail");

        JavaSparkContext context = new JavaSparkContext(conf);
        context.setLogLevel("WARN");

        try {
            // 1. Читаем CSV в Windows-1252.
            // Для небольшого файла этой ЛР читаем содержимое целиком.
            JavaRDD<List<String>> raw = context
                    .binaryFiles(args[0], 1)
                    .values()
                    .flatMap(file -> {
                        String text = new String(
                                file.toArray(),
                                Charset.forName("windows-1252")
                        );
                        return parseCsv(text).iterator();
                    })
                    .filter(row -> !row.get(0).equals("InvoiceNo"));

            long sourceCount = raw.count();

            // 2. Полные повторы восьми исходных полей удаляем ДО trim.
            // Без ID позиции это принятое допущение учебного расчёта.
            JavaRDD<Transaction> clean = raw
                    .distinct()
                    .map(RetailApp::toTransaction)
                    .cache();

            long cleanCount = clean.count();

            // 3. Оставляем положительные платные продажи.
            // C-документы, A-документы, неположительные количества и цены не входят.
            JavaRDD<Transaction> sales = clean
                    .filter(t ->
                            t.invoiceNo.matches("[0-9]{6}")
                                    && !t.stockCode.isEmpty()
                                    && !t.invoiceDate.isEmpty()
                                    && t.quantity > 0
                                    && t.unitPrice.compareTo(BigDecimal.ZERO) > 0
                    )
                    .cache();

            System.out.println("Исходных строк: " + sourceCount);
            System.out.println("После удаления повторов: " + cleanCount);
            System.out.println("Удалено повторов: " + (sourceCount - cleanCount));
            System.out.println("Строк положительных продаж: " + sales.count());

            // 4. Товарные позиции: без явных услуг и ваучеров.
            // Этот фильтр применяется к топам, но не к сумме клиентского чека.
            JavaRDD<Transaction> productSales = sales
                    .filter(t ->
                            !SERVICE_CODES.contains(
                                    t.stockCode.toUpperCase(Locale.ROOT)
                            )
                                    && !t.stockCode
                                    .toLowerCase(Locale.ROOT)
                                    .startsWith("gift_")
                    )
                    .cache();

            // Вариант 1: топ-5 только по строкам с известным клиентом.
            JavaPairRDD<String, Long> productsWithCustomers = productSales
                    .filter(t -> !t.customerId.isEmpty())
                    .mapToPair(t -> new Tuple2<>(t.stockCode, t.quantity))
                    .reduceByKey(Long::sum);

            List<Tuple2<String, Long>> top5WithCustomers =
                    productsWithCustomers.takeOrdered(5, new ProductComparator());

            System.out.println("\nТОП-5: ТОЛЬКО ИЗВЕСТНЫЕ КЛИЕНТЫ");
            System.out.println("StockCode; total_quantity");

            for (Tuple2<String, Long> product : top5WithCustomers) {
                System.out.println(product._1() + "; " + product._2());
            }

            // Вариант 2: топ-5 всех продаж, включая строки без CustomerID.
            JavaPairRDD<String, Long> productsAll = productSales
                    .mapToPair(t -> new Tuple2<>(t.stockCode, t.quantity))
                    .reduceByKey(Long::sum);

            List<Tuple2<String, Long>> top5All =
                    productsAll.takeOrdered(5, new ProductComparator());

            System.out.println("\nТОП-5: ВСЕ ПРОДАЖИ, ВКЛЮЧАЯ НЕИЗВЕСТНЫХ КЛИЕНТОВ");
            System.out.println("StockCode; total_quantity");

            for (Tuple2<String, Long> product : top5All) {
                System.out.println(product._1() + "; " + product._2());
            }

            productSales.unpersist();

            // 5. Заказы известных клиентов.
            // Суммируем все платные позиции, включая доставку и начисления.
            // Ключ заказа: (CustomerID, InvoiceNo).
            JavaPairRDD<Tuple2<String, String>, BigDecimal> orders = sales
                    .filter(t -> !t.customerId.isEmpty())
                    .mapToPair(t -> new Tuple2<>(
                            new Tuple2<>(t.customerId, t.invoiceNo),
                            t.unitPrice.multiply(
                                    BigDecimal.valueOf(t.quantity)
                            )
                    ))
                    .reduceByKey(BigDecimal::add)
                    .cache();

            // 6. Количество заказов клиента.
            JavaPairRDD<String, Long> orderCounts = orders
                    .mapToPair(order -> new Tuple2<>(
                            order._1()._1(),
                            1L
                    ))
                    .reduceByKey(Long::sum);

            // 7. Общая сумма заказов клиента, без промежуточного округления.
            JavaPairRDD<String, BigDecimal> totalSpent = orders
                    .mapToPair(order -> new Tuple2<>(
                            order._1()._1(),
                            order._2()
                    ))
                    .reduceByKey(BigDecimal::add);

            JavaPairRDD<String, Tuple2<Long, BigDecimal>> metrics =
                    orderCounts.join(totalSpent);

            // 8. Полный список известных ID, в том числе без допустимых продаж.
            JavaPairRDD<String, Integer> allCustomers = clean
                    .filter(t -> !t.customerId.isEmpty())
                    .map(t -> t.customerId)
                    .distinct()
                    .mapToPair(id -> new Tuple2<>(id, 0));

            JavaPairRDD<String, Optional<Tuple2<Long, BigDecimal>>> result =
                    allCustomers
                            .leftOuterJoin(metrics)
                            .mapValues(value -> value._2());

            System.out.println("\nМЕТРИКИ КЛИЕНТОВ");
            System.out.println(
                    "CustomerID; orders_count; total_spent; average_order_value"
            );

            // Собираем только небольшую итоговую таблицу клиентов.
            for (Tuple2<String, Optional<Tuple2<Long, BigDecimal>>> row :
                    result.sortByKey().collect()) {

                long count = 0;
                BigDecimal amount = BigDecimal.ZERO;
                String average = "NULL";

                if (row._2().isPresent()) {
                    Tuple2<Long, BigDecimal> values = row._2().get();
                    count = values._1();
                    amount = values._2();

                    // Средний чек = точная сумма / число заказов.
                    average = amount
                            .divide(
                                    BigDecimal.valueOf(count),
                                    2,
                                    RoundingMode.HALF_UP
                            )
                            .toPlainString();
                }

                System.out.println(
                        row._1() + "; "
                                + count + "; "
                                + amount.setScale(
                                        2, RoundingMode.HALF_UP
                                ).toPlainString() + "; "
                                + average
                );
            }

            System.out.println("\nЗаказов известных клиентов: " + orders.count());

        } finally {
            context.stop();
        }
    }

    private static Transaction toTransaction(List<String> row) {
        if (row.size() != 8) {
            throw new IllegalArgumentException(
                    "Ожидалось 8 колонок, получено: " + row.size()
            );
        }

        Transaction t = new Transaction();
        t.invoiceNo = row.get(0).trim();
        t.stockCode = row.get(1).trim();
        t.quantity = Long.parseLong(row.get(3).trim());
        t.invoiceDate = row.get(4).trim();
        t.unitPrice = new BigDecimal(row.get(5).trim());
        t.customerId = row.get(6).trim();
        return t;
    }

    // Запятые и переносы внутри кавычек не разделяют поля и записи.
    // Две двойные кавычки внутри поля означают одну кавычку.
    private static List<List<String>> parseCsv(String text) {
        List<List<String>> rows = new ArrayList<>();
        List<String> row = new ArrayList<>();
        StringBuilder field = new StringBuilder();
        boolean quoted = false;

        for (int i = 0; i < text.length(); i++) {
            char c = text.charAt(i);

            if (c == '"') {
                if (quoted
                        && i + 1 < text.length()
                        && text.charAt(i + 1) == '"') {
                    field.append('"');
                    i++;
                } else {
                    quoted = !quoted;
                }
            } else if (c == ',' && !quoted) {
                row.add(field.toString());
                field.setLength(0);
            } else if ((c == '\n' || c == '\r') && !quoted) {
                if (!row.isEmpty() || field.length() > 0) {
                    row.add(field.toString());
                    rows.add(row);
                }

                row = new ArrayList<>();
                field.setLength(0);

                if (c == '\r'
                        && i + 1 < text.length()
                        && text.charAt(i + 1) == '\n') {
                    i++;
                }
            } else {
                field.append(c);
            }
        }

        if (!row.isEmpty() || field.length() > 0) {
            row.add(field.toString());
            rows.add(row);
        }

        return rows;
    }

    private static class Transaction implements Serializable {
        String invoiceNo;
        String stockCode;
        String invoiceDate;
        String customerId;
        long quantity;
        BigDecimal unitPrice;
    }

    // Количество — по убыванию, StockCode при равенстве — по возрастанию.
    private static class ProductComparator
            implements Comparator<Tuple2<String, Long>>, Serializable {

        @Override
        public int compare(Tuple2<String, Long> a, Tuple2<String, Long> b) {
            int byQuantity = Long.compare(b._2(), a._2());
            return byQuantity != 0
                    ? byQuantity
                    : a._1().compareTo(b._1());
        }
    }
}
