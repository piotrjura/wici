-- Finds a device's lanes without a full scan. Every delivery round needs it.
CREATE INDEX lanes_recipient ON lanes (recipient);
