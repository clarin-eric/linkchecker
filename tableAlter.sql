#this is altering the tables according to new updates(harvestDate), you can safely delete this after the update done

#harvestdate for delete mechanism
ALTER TABLE stormychecker_dev.urls
  ADD harvestDate BIGINT DEFAULT 1000;#this default is there so it doesnt stay null and can be effectively used when calling deleteOldLinks method

#to add on delete cascade to status
ALTER TABLE stormychecker_dev.status
	DROP FOREIGN KEY status_ibfk_1;
ALTER TABLE stormychecker_dev.status 
  ADD CONSTRAINT status_ibfk_1 
  FOREIGN KEY (url) 
  REFERENCES urls(url) 
  ON DELETE CASCADE
  ON UPDATE CASCADE;


#TODO index on harvestDate

