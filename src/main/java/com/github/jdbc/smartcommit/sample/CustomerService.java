/*
 * Copyright 2020 Knut Olav Løite
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except
 * in compliance with the License. You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the License
 * is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express
 * or implied. See the License for the specific language governing permissions and limitations under
 * the License.
 */

package com.github.jdbc.smartcommit.sample;

import com.github.jdbc.smartcommit.SmartCommitConnection;
import java.math.BigDecimal;
import java.sql.Connection;
import java.sql.SQLException;
import java.time.LocalDate;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import javax.persistence.EntityManager;
import javax.persistence.PersistenceContext;
import org.hibernate.Session;
import org.hibernate.jdbc.Work;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

@Service
public class CustomerService {
  private static final Logger log = LoggerFactory.getLogger(CustomerService.class);

  static final class ConnectionLogger implements Work {
    final boolean expectedUnderlyingAutoCommitMode;

    ConnectionLogger(boolean expectedAutoCommit) {
      this.expectedUnderlyingAutoCommitMode = expectedAutoCommit;
    }

    @Override
    public void execute(Connection connection) throws SQLException {
      // Unwrap the SmartCommitConnection class.
      SmartCommitConnection smartConnection = connection.unwrap(SmartCommitConnection.class);
      // The underlying connection will be in autocommit mode when possible, and automatically
      // switch to transactional mode when necessary, and then back to autocommit after a
      // commit() or rollback().
      log.info("Underlying connection in autocommit: " + smartConnection.getDelegateAutoCommit());
      if (smartConnection.getDelegateAutoCommit() != expectedUnderlyingAutoCommitMode) {
        throw new IllegalStateException("Got unexpected autocommit mode for underlying connection");
      }
    }
  }

  @Autowired
  private CustomerRepository repository;

  @Autowired
  private PurchaseRepository purchaseRepository;

  @PersistenceContext
  private EntityManager entityManager;

  @Transactional
  public void createTestCustomers() {
    purchaseRepository.deleteAll();
    repository.deleteAll();
    repository.save(new Customer(1L, "Jack", "Bauer"));
    repository.save(new Customer(2L, "Chloe", "O'Brian"));
    repository.save(new Customer(3L, "Kim", "Bauer"));
    repository.save(new Customer(4L, "David", "Palmer"));
    repository.save(new Customer(5L, "Michelle", "Dessler"));
  }

  @Transactional
  public void createTestPurchases() {
    purchaseRepository.save(new Purchase(1L, repository.findById(1L), LocalDate.of(2010, 2, 9),
        "Product 1", new BigDecimal("10.99")));
    purchaseRepository.save(new Purchase(2L, repository.findById(1L), LocalDate.of(2010, 2, 9),
        "Product 2", new BigDecimal("2.50")));
    purchaseRepository.save(new Purchase(3L, repository.findById(2L), LocalDate.of(2018, 12, 23),
        "Product 5", new BigDecimal("110.34")));
    purchaseRepository.save(new Purchase(4L, repository.findById(2L), LocalDate.of(2018, 11, 1),
        "Product 2", new BigDecimal("2.75")));
    purchaseRepository.save(new Purchase(5L, repository.findById(2L), LocalDate.of(2019, 1, 3),
        "Product 6", new BigDecimal("8.69")));
    purchaseRepository.save(new Purchase(6L, repository.findById(3L), LocalDate.of(2019, 1, 4),
        "Product 1", new BigDecimal("12.99")));
  }

  @Transactional
  public void queryAndUpdate() {
    // This method is marked as Transactional and will join any existing transaction or create a new
    // one if needed. As it is called from the non-transactional method demo() in this test class,
    // it will create a new transaction.

    // The SmartCommit JDBC driver will however not actually start a transaction on the underlying
    // database before it encounters a write operation. That means that the following query
    // statements will all be executed in autocommit mode, and hence not take any read locks.

    // Get the Hibernate session and create a Work that can log whether the underlying database
    // connection is in autocommit mode or not.
    Session session = (Session) entityManager.getDelegate();
    // The underlying connection should be in autocommit, although this is a transactional method.
    log.info("Expecting autocommit=true for underlying connection");
    session.doWork(new ConnectionLogger(true));

    // fetch all customers
    log.info("Customers found with findAll():");
    log.info("-------------------------------");
    for (Customer customer : repository.findAll()) {
      log.info(customer.toString());
    }
    log.info("");
    // The underlying connection should still be in autocommit mode.
    log.info("Expecting autocommit=true for underlying connection");
    session.doWork(new ConnectionLogger(true));

    // fetch an individual customer by ID
    Customer c = repository.findById(1L);
    log.info("Customer found with findById(1L):");
    log.info("--------------------------------");
    log.info(c.toString());
    log.info("");
    // The underlying connection should still be in autocommit mode.
    log.info("Expecting autocommit=true for underlying connection");
    session.doWork(new ConnectionLogger(true));

    // fetch customers by last name
    log.info("Customer found with findByLastName('Bauer'):");
    log.info("--------------------------------------------");
    repository.findByLastName("Bauer").forEach(bauer -> {
      log.info(bauer.toString());
    });
    log.info("");
    // The underlying connection should still be in autocommit mode.
    log.info("Expecting autocommit=true for underlying connection");
    session.doWork(new ConnectionLogger(true));

    // Create a few purchases. This will be executed in the same transaction as the transaction that
    // was started by this method. The fact that the SmartCommit driver has not yet actually started
    // a transaction on the database server is not visible to the client application. This action
    // will however (eventually) start a transaction on the server.
    log.info("Creating test purchases");
    log.info("");
    createTestPurchases();
    // The default flush mode of Hibernate is AUTO. This means that the createTestPurchases() method
    // was joined with the current transaction, but the update statements have not yet been
    // executed. That will happen when Hibernate performs a flush, i.e. when a commit or a query is
    // executed. The underlying connection will therefore still be in autocommit mode. Note: That
    // does NOT mean that the update statements will be executed in autocommit mode. The connection
    // will turn OFF autocommit BEFORE the update statements are executed.
    log.info("Expecting autocommit=true for underlying connection");
    session.doWork(new ConnectionLogger(true));

    // Now execute a query. This will trigger a flush from Hibernate, and switch the underlying
    // connection to transactional.
    long purchases = purchaseRepository.count();
    log.info("Number of purchases found: " + purchases);
    log.info("");
    // The underlying connection should now be in transactional mode.
    log.info("Expecting autocommit=false for underlying connection");
    session.doWork(new ConnectionLogger(false));

    // Now try to update the total spent amount for all customers. This method is also marked as
    // @Transactional and will join the current transaction. It will therefore also see the changes
    // that this transaction has made.
    log.info("Updating total spent values");
    log.info("");
    updateCustomerSpending(repository.findAll());
    // The underlying connection will still be in transactional mode.
    log.info("Expecting autocommit=false for underlying connection");
    session.doWork(new ConnectionLogger(false));

    log.info("Customers with updated total spending:");
    log.info("-------------------------------");
    for (Customer customer : repository.findAll()) {
      log.info(customer.toString());
    }
    log.info("");
    // The underlying connection will still be in transactional mode, as the previous read operation
    // was executed *after* a write operation in a transaction. This read operation cannot be
    // executed in autocommit mode, as it would not be able to see the uncommitted changes of this
    // transaction.
    log.info("Expecting autocommit=false for underlying connection");
    session.doWork(new ConnectionLogger(false));
  }

  @Transactional
  public void updateCustomerSpending(Iterable<Customer> customers) {
    // Fetch the purchases for all the given customers before we start writing.
    // This will postpone the actual underlying transaction as long as possible, as the
    // read operations can be executed outside the transaction if there is not already an active
    // transaction.
    Map<Customer, List<Purchase>> purchases = new HashMap<Customer, List<Purchase>>();
    for (Customer customer : customers) {
      purchases.put(customer, purchaseRepository.findByCustomer(customer));
    }
    // Update the totalSpent field of the customers.
    for (Entry<Customer, List<Purchase>> entry : purchases.entrySet()) {
      Customer customer = entry.getKey();
      List<Purchase> customerPurchases = entry.getValue();
      BigDecimal totalSpent = BigDecimal.ZERO;
      for (Purchase purchase : customerPurchases) {
        totalSpent = totalSpent.add(purchase.getAmount());
      }
      customer.setTotalSpent(totalSpent);
      repository.save(customer);
    }
  }

  @Transactional
  public void queryAllCustomers() {
    // Get the Hibernate session and create a Work that can log whether the underlying database
    // connection is in autocommit mode or not.
    Session session = (Session) entityManager.getDelegate();
    log.info("Expecting autocommit=true for underlying connection");
    session.doWork(new ConnectionLogger(true));

    log.info("Customers with updated total spending:");
    log.info("-------------------------------");
    for (Customer customer : repository.findAll()) {
      log.info(customer.toString());
    }
    log.info("");

    log.info("Expecting autocommit=true for underlying connection");
    session.doWork(new ConnectionLogger(true));
  }
}
