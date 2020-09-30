/*
 * Copyright 2020 Knut Olav Løite
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *       http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.github.jdbc.smartcommit.sample;

import org.springframework.boot.CommandLineRunner;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.context.annotation.Bean;

@SpringBootApplication
public class SmartCommitSampleApplication {
  public static void main(String[] args) {
    SpringApplication.run(SmartCommitSampleApplication.class);
  }

  @Bean
  public CommandLineRunner run(CustomerService service) {
    return (args) -> {
      // Create a few customers. This will be executed in a transaction as the service method is
      // marked as Transactional.
      service.createTestCustomers();

      // Do some test querying and updating. The method is marked as @Transactional, but the queries
      // at the beginning of the method will be executed in autocommit mode. The JDBC driver will
      // automatically switch to transactional mode when the first write operation is encountered.
      service.queryAndUpdate();

      // Now execute a query in transactional mode. The underlying connection will remain in
      // autocommit=true mode.
      service.queryAllCustomers();
    };
  }
}
