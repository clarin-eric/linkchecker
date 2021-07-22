# the script creates a linkchecker_test database with some records from the ASV_Leipzig provider group 
# from the linkchecker database. We agreed that all URLs which contain the string 'corpusId' 
# are excluded from testing, since they cause an unneccessary load on server side. 
# author: Wolfgang Walter SAUER (wolfgang.sauer@oeaw.ac.at)

DROP DATABASE IF EXISTS `linkchecker_test`;
CREATE DATABASE  IF NOT EXISTS `linkchecker_test` CHARACTER SET utf8 COLLATE utf8_general_ci;
USE `linkchecker_test`;

CREATE USER IF NOT EXISTS 'testUser'@'%' IDENTIFIED BY 'testPassword';
GRANT ALL ON `linkchecker_test`.* TO 'testUser'@'%';

CREATE TABLE `providerGroup` (
  `id` int NOT NULL AUTO_INCREMENT,
  `name` varchar(256) NOT NULL,
  PRIMARY KEY (`id`),
  UNIQUE KEY `idx_name` (`name`)
);


CREATE TABLE `context` (
  `id` int NOT NULL AUTO_INCREMENT,
  `source` varchar(256) DEFAULT NULL,
  `record` varchar(256) DEFAULT NULL,
  `providerGroup_id` int DEFAULT NULL,
  `expectedMimeType` varchar(256) DEFAULT NULL,
  PRIMARY KEY (`id`),
  UNIQUE KEY `idx_record_providerGroup_id_expectedMimeType` (`record`,`providerGroup_id`, `expectedMimeType`)
);



CREATE TABLE `url` (
  `id` int NOT NULL AUTO_INCREMENT,
  `url` varchar(1024) CHARACTER SET ascii COLLATE ascii_bin NOT NULL,
  PRIMARY KEY (`id`),
  UNIQUE KEY `idx_url` (`url`)
);


CREATE TABLE `url_context` (
  `id` int NOT NULL AUTO_INCREMENT,
  `url_id` int NOT NULL,
  `context_id` int NOT NULL,
  `ingestionDate` datetime NOT NULL DEFAULT NOW(),
  `active` boolean NOT NULL DEFAULT false,
  PRIMARY KEY (`id`),
  KEY `fk_url_context_1_idx` (`url_id`),
  KEY `fk_url_context_2_idx` (`context_id`),
  CONSTRAINT `fk_url_context_1` FOREIGN KEY (`url_id`) REFERENCES `url` (`id`),
  CONSTRAINT `fk_url_context_2` FOREIGN KEY (`context_id`) REFERENCES `context` (`id`)
);


CREATE TABLE `status` (
  `id` int NOT NULL AUTO_INCREMENT,
  `url_id` int DEFAULT NULL,
  `statusCode` int DEFAULT NULL,
  `message` varchar(256),
  `category` varchar(25) NOT NULL,
  `method` varchar(10) NOT NULL,
  `contentType` varchar(256) DEFAULT NULL,
  `byteSize` int DEFAULT NULL,
  `duration` int DEFAULT NULL,
  `checkingDate` datetime NOT NULL,
  `redirectCount` int DEFAULT NULL,
  PRIMARY KEY (`id`),
  UNIQUE KEY `idx_url_id` (`url_id`),
  KEY `fk_status_1_idx` (`url_id`),
  CONSTRAINT `fk_status_1` FOREIGN KEY (`url_id`) REFERENCES `url` (`id`)
);


CREATE TABLE `history` (
  `id` int NOT NULL AUTO_INCREMENT,
  `status_id` int NOT NULL,
  `url_id` int DEFAULT NULL,
  `statusCode` int DEFAULT NULL,
  `message` varchar(256),
  `category` varchar(25) NOT NULL,
  `method` varchar(10) NOT NULL,
  `contentType` varchar(256) DEFAULT NULL,
  `byteSize` int DEFAULT NULL,
  `duration` int DEFAULT NULL,
  `checkingDate` datetime NOT NULL,
  `redirectCount` int DEFAULT NULL,
  PRIMARY KEY (`id`),
  UNIQUE KEY `idx_url_id_ceckingDate` (`url_id`,`checkingDate`)
);

INSERT INTO linkchecker_test.providerGroup
SELECT * FROM linkchecker.providerGroup p
WHERE p.name = 'ASV_Leipzig';

INSERT INTO linkchecker_test.context
SELECT c.* FROM linkchecker.context c, linkchecker.providerGroup p
WHERE p.name = 'ASV_Leipzig'
AND c.providerGroup_id = p.id;

INSERT INTO linkchecker_test.url
SELECT DISTINCT u.* FROM linkchecker.url u, linkchecker.url_context uc, linkchecker.context c, linkchecker.providerGroup p
WHERE p.name = 'ASV_Leipzig'
AND u.url NOT LIKE '%corpusId=%'
AND c.providerGroup_id = p.id
AND uc.context_id=c.id
AND u.id=uc.url_id;

INSERT INTO linkchecker_test.url_context
SELECT uc.* FROM linkchecker.url_context uc
WHERE uc.url_id IN 
(SELECT DISTINCT u.id FROM linkchecker.url u, linkchecker.url_context uc, linkchecker.context c, linkchecker.providerGroup p
WHERE p.name = 'ASV_Leipzig'
AND u.url NOT LIKE '%corpusId=%'
AND c.providerGroup_id = p.id
AND uc.context_id=c.id
AND u.id=uc.url_id);