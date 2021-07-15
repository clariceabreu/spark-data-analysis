# Data Analysis Using Apache Spark

## How to run
>To run this project it is required that you have maven installed.
To check if it is installed run the command:
`mvn --version`
If it is not installed you can see how to install it [here](https://maven.apache.org/install.html).

1. Build the project
```bash
mvn package
```

2. Export Spark Driver address
```bash
export SPARK_LOCAL_IP="127.0.0.1"
```

3. Compile and run
```bash
mvn clean compile exec:jav
```
