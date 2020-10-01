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

import java.math.BigDecimal;
import javax.persistence.Entity;
import javax.persistence.Id;

@Entity
public class Customer {

  @Id
  private Long id;
  private String firstName;
  private String lastName;
  private BigDecimal totalSpent;

  protected Customer() {}

  public Customer(Long id, String firstName, String lastName) {
    this.id = id;
    this.firstName = firstName;
    this.lastName = lastName;
  }

  @Override
  public String toString() {
    return String.format("Customer[id=%d, firstName='%s', lastName='%s', totalSpent=%s]", id,
        firstName, lastName, totalSpent);
  }

  public Long getId() {
    return id;
  }

  public String getFirstName() {
    return firstName;
  }

  public String getLastName() {
    return lastName;
  }

  public BigDecimal getTotalSpent() {
    return totalSpent;
  }

  public void setTotalSpent(BigDecimal totalSpent) {
    this.totalSpent = totalSpent;
  }
}
