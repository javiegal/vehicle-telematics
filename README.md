# Vehicle Telematics

## Description

[Apache Flink](https://flink.apache.org/) project made for the Cloud Computing and Big Data Ecosystems course of the Master's Programme in Data Science of the UPM.

The program takes as input a file with specific format ([example file](https://dl.lsdupm.ovh/CLOUD/2122/sample-traffic-3xways.csv)) and computes some speed and accident reports. A detailed description of the program can be seen in <a href="./project.pdf" target="_blank">project.pdf</a>.

## Requirements
This project has been developed with the following software:
- Oracle Java 11
- Apache Maven 3.6.3
- Apache Flink 1.14.0

The program has been configured with the `flink-quickstart-java` maven artifact and optimized to run on a Flink cluster with 3 task manager slots.

## Installation
Compile and package it using Maven as follows:
```
mvn clean package
```
A `vehicle-telematics-1.0-SNAPSHOT.jar` file will be created under a `target` directory.

## Usage
Start a Flink cluster (run `./start-cluster.sh` in the directory where Flink is installed) and run the following command:
```
flink run -c master.VehicleTelematics target/vehicle-telematics-1.0-SNAPSHOT.jar $INPUT_FILE $OUTPUT_FOLDER
```
The selected `$OUTPUT_FOLDER` will contain the files with the results.


## Authors
- Javier Gallego Gutiérrez ([@javiegal](https://github.com/javiegal))
- Ricardo María Longares Díez ([@RLongares](https://github.com/Rlongares))
