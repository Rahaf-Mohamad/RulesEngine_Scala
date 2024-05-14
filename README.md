# Discount Calculator

## Overview

Discount Calculator is a Scala program designed to process orders from a CSV file, calculate discounts based on various criteria, and save the processed orders to both a database and a CSV file. This README provides an overview of the program's functions and usage instructions.

### Functions

#### Modular Design

The code is organized into functions for improved readability, maintainability, and extensibility.

#### Discount Calculation

Discounts are calculated based on the following criteria:

- **Days to Expiry**: Orders with fewer than or equal to 29 days remaining are eligible for a discount.
- **Product Category**: Orders containing products categorized as "Cheese" or "Wine" receive specific discounts.
- **Date**: Orders placed on March 23rd receive a specific discount.
- **Quantity**: Orders with quantities exceeding certain thresholds receive quantity-based discounts.
- **Payment Method**: Orders paid via Visa card receive a specific discount.

### Usage

#### Prepare Input Data

1. Prepare a CSV file (`TRX1000.csv`) containing order data. Each row should represent an order, and the columns should include `timestamp`, `product`, `expiryDate`, `quantity`, `unitPrice`, `channel`, and `paymentMethod`.

#### Configure Database Connection

1. Ensure that an Oracle database instance is running and accessible.
2. Modify the `url`, `username`, and `password` variables in the code to connect to the desired Oracle database instance.

#### Compile and Run

1. Compile the Scala code using SBT (`sbt compile`).
2. Run the program using SBT (`sbt run`).

### View Results

Processed orders will be saved to both a database table (`Scala_Orders`) and a CSV file (`discounted_transactions.csv`). Check the database and the CSV file to view the processed orders and their respective discounts.

#### Configuration

Modify the database connection settings and any other necessary configurations in the code to suit your environment and requirements.
