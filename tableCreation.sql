CREATE TABLE stormychecker.urls (
 url VARCHAR(255),
 nextfetchdate TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
 host VARCHAR(128),
 record VARCHAR(255),
 collection VARCHAR(255),
 expectedMimeType VARCHAR(255),
 harvestDate BIGINT(255),
 PRIMARY KEY(url)
);

CREATE INDEX record ON stormychecker.urls (record);
CREATE INDEX collection ON stormychecker.urls (collection);
CREATE INDEX collection_record ON stormychecker.urls (collection,record);
CREATE INDEX collection_record_url ON stormychecker.urls (collection,record,url);
CREATE INDEX record_url ON stormychecker.urls (record,url);

CREATE TABLE stormychecker.status (
 url VARCHAR(255) UNIQUE,
 statusCode INT,
 method VARCHAR(128),
 contentType VARCHAR(255),
 byteSize INT,
 duration INT,
 timestamp TIMESTAMP,
 redirectCount INT,
 record VARCHAR(255),
 collection VARCHAR(255),
 expectedMimeType VARCHAR(255),
 message VARCHAR(255),
 category VARCHAR(50),
 FOREIGN KEY (url) REFERENCES urls (url) ON DELETE CASCADE ON UPDATE CASCADE
);

CREATE INDEX statusCode ON stormychecker.status (statusCode);
CREATE INDEX statusCode_url ON stormychecker.status (statusCode,url);
CREATE INDEX record ON stormychecker.status (record);
CREATE INDEX collection ON stormychecker.status (collection);
CREATE INDEX collection_record ON stormychecker.status (collection,record);
CREATE INDEX collection_record_url ON stormychecker.status (collection,record,url);
CREATE INDEX record_url ON stormychecker.status (record,url);

CREATE TABLE stormychecker.history (
 url VARCHAR(255),
 statusCode INT,
 method VARCHAR(128),
 contentType VARCHAR(255),
 byteSize INT,
 duration INT,
 timestamp TIMESTAMP,
 redirectCount INT,
 record VARCHAR(255),
 collection VARCHAR(255),
 expectedMimeType VARCHAR(255),
 message VARCHAR(255),
 category VARCHAR(50)
);

CREATE TABLE stormychecker.metrics (
 srcComponentId VARCHAR(128),
 srcTaskId INT,
 srcWorkerHost VARCHAR(128),
 srcWorkerPort INT,
 name VARCHAR(128),
 value DOUBLE,
 timestamp TIMESTAMP
);