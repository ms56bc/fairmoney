# Use an official Maven runtime as a parent image
FROM maven:3.8.5-openjdk-11-slim AS build

# Set the working directory in the container
WORKDIR /fairmoney

# Package the application
RUN mvn package
