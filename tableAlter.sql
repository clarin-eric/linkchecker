#this is altering the tables according to new updates(harvestDate), you can safely delete this after the update done

ALTER TABLE stormychecker.urls
  ADD harvestDate DATE;

ALTER TABLE stormychecker.status
	DROP CONSTRAINT status_ibfk_1;

ALTER TABLE stormychecker.status 
  ADD CONSTRAINT status_ibfk_1 
  FOREIGN KEY (url) 
  REFERENCES urls(url) 
  ON DELETE CASCADE
  ON UPDATE CASCADE;


