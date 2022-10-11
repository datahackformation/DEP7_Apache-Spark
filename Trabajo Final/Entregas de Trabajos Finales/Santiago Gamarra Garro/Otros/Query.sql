CREATE DATABASE /*!32312 IF NOT EXISTS*/`sgamarrag` /*!40100 DEFAULT CHARACTER SET latin1 */;
USE `sgamarrag`;

DROP TABLE `order_items`;

CREATE TABLE `order_items` (
    `order_item_id` int(11) NOT NULL,
    `order_item_order_id` int(11) NOT NULL,
    `order_item_product_id` int(11) NOT NULL,
    `order_item_quantity` smallint(4) NOT NULL,
    `order_item_subtotal` decimal(10,2) DEFAULT NULL,
    `order_item_product_price` decimal(10,2) DEFAULT NULL,
    PRIMARY KEY (`order_item_id`),
    KEY `order_item_order_id` (`order_item_order_id`),
    KEY `order_item_product_id` (`order_item_product_id`),
    CONSTRAINT `order_items_ibfk_1` FOREIGN KEY (`order_item_order_id`) REFERENCES `orders` (`order_id`),
    CONSTRAINT `order_items_ibfk_2` FOREIGN KEY (`order_item_product_id`) REFERENCES `products` (`product_id`)
) ENGINE=InnoDB DEFAULT CHARSET=latin1;

DROP TABLE `orders`;

CREATE TABLE `orders` (
    `order_id` int(11) NOT NULL,
    `order_date` date NOT NULL,
    `order_customer_id` int(11) NOT NULL,
    `order_status` varchar(45) DEFAULT NULL,
    PRIMARY KEY (`order_id`),
    KEY `order_customer_id` (`order_customer_id`),
    CONSTRAINT `orders_ibfk_1` FOREIGN KEY (`order_customer_id`) REFERENCES `customers` (`customer_id`)
) ENGINE=InnoDB DEFAULT CHARSET=latin1;

DROP TABLE `customers`;

CREATE TABLE `customers` (
    `customer_id` int(11) NOT NULL,
    `customer_fname` varchar(45) DEFAULT NULL,
    `customer_lname` varchar(45) DEFAULT NULL,
    `customer_email` varchar(45) NOT NULL,
    `customer_password` varchar(45) NOT NULL,
    `customer_street` varchar(45) DEFAULT NULL,
    `customer_city` varchar(45) DEFAULT NULL,
    `customer_state` varchar(45) DEFAULT NULL,
    `customer_zipcode` varchar(45) DEFAULT NULL,
    PRIMARY KEY (`customer_id`)
) ENGINE=InnoDB DEFAULT CHARSET=latin1;

DROP TABLE  `products`;

CREATE TABLE `products` (
    `product_id` int(11) NOT NULL,
    `product_category_id` int(11) NOT NULL,
    `product_name` varchar(45) DEFAULT NULL,
    `product_description` varchar(45) DEFAULT NULL,
    `product_price` decimal(10,2) NOT NULL,
    `product_image` varchar(255) DEFAULT NULL,
    PRIMARY KEY (`product_id`),
    KEY `product_category_id` (`product_category_id`),
    CONSTRAINT `products_ibfk_1` FOREIGN KEY (`product_category_id`) REFERENCES `categories` (`category_id`)
) ENGINE=InnoDB DEFAULT CHARSET=latin1;

DROP TABLE `categories`;

CREATE TABLE `categories` (
    `category_id` int(11) NOT NULL,
    `category_department_id` int(11) NOT NULL,
    `category_name` varchar(45) DEFAULT NULL,
    PRIMARY KEY (`category_id`),
    KEY `category_department_id` (`category_department_id`),
    CONSTRAINT `categories_ibfk_1` FOREIGN KEY (`category_department_id`) REFERENCES `departments` (`department_id`)
) ENGINE=InnoDB DEFAULT CHARSET=latin1;

DROP TABLE `departments`;

CREATE TABLE `departments` (
    `department_id` int(11) NOT NULL,
    `department_name` varchar(45) NOT NULL,
    PRIMARY KEY (`department_id`)
) ENGINE=InnoDB DEFAULT CHARSET=latin1;

