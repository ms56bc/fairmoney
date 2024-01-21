# Use an official OpenJDK image as the base image
FROM openjdk:11-jre-slim
WORKDIR /opt/app
COPY target/transactions-producer.jar .
CMD ["java","-jar","transactions-producer.jar"]