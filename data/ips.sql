USE test_db;
CREATE TABLE ips (start_ip VARCHAR(20), end_ip VARCHAR(20));
INSERT INTO ips (start_ip, end_ip)
    VALUES     ("197.203.0.0", "197.206.9.255"),
               ("197.204.0.0", "197.204.0.24"),
               ("201.233.7.160", "201.233.7.168"),
               ("201.233.7.164", "201.233.7.168"),
               ("201.233.7.167", "201.233.7.167"),
               ("203.133.0.0", "203.133.255.255");