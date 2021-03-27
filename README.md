# Project Title

Login fail attempt count detection: This project counts the number of failed attempt done by a particular username to log in into a system. It uses **Kafka Streams** and performs a **windowed aggregation**.

# Built With
* Kafka Stream API
* Spring Boot
* Maven
* Docker Confluent Platform 

## Getting started
Running the application with the **dev profile**:
  * In the **init.properties** file:
    * Change the value of the parameter **profile** to **dev** if it is not done already
    * Update the src.dir with the path of the source code where you intend to clone the project
  
#### Windows Os:
Open a Command line window
Create a folder where you want to clone the project
Clone the project 
```bash
git clone https://github.com/enemes2000/loginfailedattempt.git
```
Launch the application
```bash
start.bat
```
Type this on your browser or use curl
To view all attempts
```bash
http://localhost:8080/login/failattempt/all
```
You should see this result:
```bash
[{"username":"mike","failattemptCount":4,"ipAddress":"205.23.45.45"},{"username":"andrew","failattemptCount":5,"ipAddress":"190.23.45.45"}]
```

To view the username: mike or andrew attempts
```bash
http://localhost:8080/login/failattempt?username=mike
```
You should see this result
```bash
[{"username":"mike","failattemptCount":4,"ipAddress":"205.23.45.45"}]
```
### Prerequisites
* Docker
* Java
* Maven
* Docker Compose

## Running the tests
* Change the **profile** variable to **test** in the **init.properties** file
* Launch your test by typing on your command line console

```bash
start.bat
```

