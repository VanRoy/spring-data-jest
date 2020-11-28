Spring Data Jest
=========================
[![Apache License 2](https://img.shields.io/badge/license-ASF2-blue.svg)](https://www.apache.org/licenses/LICENSE-2.0.txt)
[![Build Status](https://travis-ci.org/VanRoy/spring-data-jest.svg?branch=master)](https://travis-ci.org/VanRoy/spring-data-jest)
[![Maven Central](https://maven-badges.herokuapp.com/maven-central/com.github.vanroy/spring-boot-starter-data-jest/badge.svg?style=default)](https://maven-badges.herokuapp.com/maven-central/com.github.vanroy/spring-boot-starter-data-jest/)

Spring Data implementation for ElasticSearch based on Jest Rest client

Useful to use Spring Data with ElasticSearch cluster accessible only by HTTP ( for example on AWS).

Versions
-----------

|   spring data jest |      spring boot     | spring data elasticsearch |  jest  | elasticsearch |
|:------------------:|:--------------------:|:-------------------------:|:------:|:-------------:|
|   3.3.4.RELEASE    |         2.2.1        |       3.2.1.RELEASE       |  6.3.1 |      6.8.4    |
|   3.2.5.RELEASE    |         2.1.2        |       3.1.3.RELEASE       |  6.3.1 |      6.4.3    |
|   3.1.5.RELEASE    |         2.0.5        |      3.0.10.RELEASE       |  5.3.4 |      5.5.0    |
|   3.0.0.RELEASE    |       2.0.0.M4       |       3.0.0.RELEASE       |  5.3.2 |      5.5.0    |
|   2.3.1.RELEASE    |         1.5.x        |       2.1.0.RELEASE       |  2.0.4 |      2.4.4    |
|   2.2.0.RELEASE    |       >= 1.4.3       |       2.0.6.RELEASE       |  2.0.4 |      2.4.3    |
|   2.1.4.RELEASE    |        < 1.4.3       |       2.0.5.RELEASE       |  2.0.3 |      2.2.0    |
|   1.0.2.RELEASE    |         1.3.x        |       1.3.4.RELEASE       |  1.0.3 |      1.5.2    |


Quick Start with Spring Boot
-------------------------------

### Maven configuration

Add the Maven dependency:

```xml
<dependency>
    <groupId>com.github.vanroy</groupId>
    <artifactId>spring-boot-starter-data-jest</artifactId>
    <version>3.3.4.RELEASE</version>
</dependency>

<!-- Only required for start local ES node -->
<dependency>
    <groupId>org.apache.logging.log4j</groupId>
    <artifactId>log4j-core</artifactId>
</dependency>
```

For specific case of AWS ES Domain (with Request Signing) add this dependency:

```xml
<dependency>
    <groupId>vc.inreach.aws</groupId>
    <artifactId>aws-signing-request-interceptor</artifactId>
    <version>0.0.21</version>
</dependency>
```

### Application
```java
@SpringBootApplication(exclude = {ElasticsearchAutoConfiguration.class, ElasticsearchDataAutoConfiguration.class})
public class Application {
    public static void main(String[] args) {
        SpringApplication.run(Application.class, args);
    }
}
```

### Configuration ( application.yml )
```yml
spring:
    data:
        jest:
            uri: http://localhost:9200

            # Useful only for AWS request signing ( not required on EC2 instance, it's auto-discovered )
            aws-region: eu-west-1
```


Quick Start without Spring Boot
-------------------------------

### Maven configuration

Add the Maven dependency:

```xml
<dependency>
    <groupId>com.github.vanroy</groupId>
    <artifactId>spring-data-jest</artifactId>
    <version>3.3.4.RELEASE</version>
</dependency>

<!-- Only required for start local / embedded ES node -->
<dependency>
    <groupId>org.apache.logging.log4j</groupId>
    <artifactId>log4j-core</artifactId>
</dependency>
```

### XML Namespace

You can set up repository scanning via xml configuration, which will happily create your repositories.

```xml
<?xml version="1.0" encoding="UTF-8"?>
<beans xmlns="http://www.springframework.org/schema/beans"
       xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance"
       xmlns:elasticsearch="http://www.springframework.org/schema/data/elasticsearch"
       xsi:schemaLocation="http://www.springframework.org/schema/beans http://www.springframework.org/schema/beans/spring-beans-3.1.xsd http://www.springframework.org/schema/data/elasticsearch http://www.springframework.org/schema/data/elasticsearch/spring-elasticsearch.xsd">

    <!-- Elastic search test node -->
    <elasticsearch:node-client id="client" local="true" cluster-name="#{T(java.util.UUID).randomUUID().toString()}"
                               http-enabled="true" path-data="target/elasticsearchTestData" path-home="src/test/resources/test-home-dir"
                               path-configuration="node-client-configuration.yml"/>


    <!-- ElasticSearch Jest Client -->
    <bean id="jestClient" factory-bean="jestClientFactory" factory-method="getObject" destroy-method="close" />

    <bean id="jestClientConfigBuilder" class="io.searchbox.client.config.HttpClientConfig.Builder">
        <constructor-arg type="java.lang.String" value="http://localhost:9200"/>
    </bean>

    <bean id="jestClientConfig" factory-bean="jestClientConfigBuilder" factory-method="build"/>

    <bean id="jestClientFactory" class="io.searchbox.client.JestClientFactory">
        <property name="httpClientConfig" ref="jestClientConfig"/>
    </bean>

    <!-- Elasticsearch Jest Template -->
    <bean id="jestElasticsearchTemplate" class="com.github.vanroy.springdata.jest.JestElasticsearchTemplate">
        <constructor-arg ref="jestClient"/>
    </bean>

</beans>
```

Samples
-------------------------------

Sample with Spring Boot and embedded Elasticsearch :

[https://github.com/VanRoy/spring-data-jest/tree/master/spring-boot-sample-data-jest](https://github.com/VanRoy/spring-data-jest/tree/master/spring-boot-sample-data-jest)

Sample with Spring Boot and Elasticsearch on AWS (managed) :

[https://github.com/VanRoy/spring-data-jest/tree/master/spring-boot-sample-data-jest-aws](https://github.com/VanRoy/spring-data-jest/tree/master/spring-boot-sample-data-jest-aws)

Plugins loading
-------------------------------

All plugins located in `org.elasticsearch.plugin` are loaded by default.
All plugins located in packages specified in property `spring.data.jest.pluginPackages` are also loaded.

### Example for Expression-lang
In pom.xml, add plugin dependency :
````xml
 <dependency>
   <groupId>org.codelibs.elasticsearch.module</groupId>
   <artifactId>lang-expression</artifactId>
   <version>...</version>
 </dependency>
````
In application.yml, add following :
```yml
spring:
    data:
        jest:
            pluginPackages: org.elasticsearch.script.expression
```


Sponsors
--------

### JetBrains

[![JetBrains](jetbrains.png)]( https://www.jetbrains.com/?from=SpringDataJest)

[JetBrains](https://www.jetbrains.com/?from=SpringDataJest) supports our open source project by sponsoring a [All Products Packs](https://www.jetbrains.com/products.html?from=SpringDataJest) within their [Free Open Source License](https://www.jetbrains.com/buy/opensource/?from=SpringDataJest) program.
