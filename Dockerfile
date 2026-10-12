FROM apache/spark:4.0.1-scala2.13-java17-ubuntu AS build

USER root
WORKDIR /build

COPY src/main/java ./src

RUN mkdir classes \
    && javac --release 17 -cp "/opt/spark/jars/*" -d classes \
        src/ru/example/*.java \
    && jar --create --file online-retail-loader.jar -C classes .

FROM apache/spark:4.0.1-scala2.13-java17-ubuntu

COPY --from=build /build/online-retail-loader.jar /opt/spark/work-dir/online-retail-loader.jar
COPY data.csv /data/data.csv

ENTRYPOINT ["/opt/spark/bin/spark-submit"]
CMD ["--master", "local[1]", "--class", "ru.example.RetailApp", "/opt/spark/work-dir/online-retail-loader.jar", "/data/data.csv", "/output"]
