CREATE DATABASE IF NOT EXISTS raw;
USE raw;

-- raw.customers definition

CREATE TABLE `customers` (
  `customerNumber` bigint DEFAULT NULL,
  `customerName` text,
  `contactLastName` text,
  `contactFirstName` text,
  `phone` text,
  `addressLine1` text,
  `addressLine2` text,
  `city` text,
  `state` text,
  `postalCode` text,
  `country` text,
  `salesRepEmployeeNumber` double DEFAULT NULL,
  `creditLimit` double DEFAULT NULL
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;


-- raw.employees definition

CREATE TABLE `employees` (
  `employeeNumber` bigint DEFAULT NULL,
  `lastName` text,
  `firstName` text,
  `extension` text,
  `email` text,
  `officeCode` text,
  `reportsTo` double DEFAULT NULL,
  `jobTitle` text
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;


-- raw.offices definition

CREATE TABLE `offices` (
  `officeCode` text,
  `city` text,
  `phone` text,
  `addressLine1` text,
  `addressLine2` text,
  `state` text,
  `country` text,
  `postalCode` text,
  `territory` text
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;


-- raw.orderdetails definition

CREATE TABLE `orderdetails` (
  `orderNumber` bigint DEFAULT NULL,
  `productCode` text,
  `quantityOrdered` bigint DEFAULT NULL,
  `priceEach` double DEFAULT NULL,
  `orderLineNumber` bigint DEFAULT NULL
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;


-- raw.orders definition

CREATE TABLE `orders` (
  `orderNumber` bigint DEFAULT NULL,
  `orderDate` date DEFAULT NULL,
  `requiredDate` date DEFAULT NULL,
  `shippedDate` date DEFAULT NULL,
  `status` text,
  `comments` text,
  `customerNumber` bigint DEFAULT NULL
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;


-- raw.payments definition

CREATE TABLE `payments` (
  `customerNumber` bigint DEFAULT NULL,
  `checkNumber` text,
  `paymentDate` date DEFAULT NULL,
  `amount` double DEFAULT NULL
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;


-- raw.productlines definition

CREATE TABLE `productlines` (
  `productLine` text,
  `textDescription` text,
  `htmlDescription` text,
  `image` text
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;


-- raw.products definition

CREATE TABLE `products` (
  `productCode` text,
  `productName` text,
  `productLine` text,
  `productScale` text,
  `productVendor` text,
  `productDescription` text,
  `quantityInStock` bigint DEFAULT NULL,
  `buyPrice` double DEFAULT NULL,
  `MSRP` double DEFAULT NULL
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;
