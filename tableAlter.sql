ALTER TABLE stormychecker_dev.status
  ADD category VARCHAR(50);

ALTER TABLE stormychecker_dev.history
  ADD category VARCHAR(50);

#TODO index on harvestDate

