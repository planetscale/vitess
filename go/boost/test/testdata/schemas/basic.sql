CREATE TABLE a (
	c1 bigint NOT NULL,
	c2 bigint NOT NULL,
	PRIMARY KEY (c1)
);

CREATE TABLE b (
	c1 bigint NOT NULL,
	c2 bigint NOT NULL,
	PRIMARY KEY (c1)
);

SELECT /*vt+ VIEW=c */ * FROM (SELECT c1, c2 FROM a UNION SELECT c1, c2 FROM b) AS u WHERE u.c1 = :a;
