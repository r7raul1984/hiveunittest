
USE ${hiveconf:my.schema};


 CREATE TEMPORARY MACRO clean(content string)
  CASE
    WHEN trim(content) = "Hello" THEN "Hellojj"
    ELSE content
  END;

CREATE TABLE foo_prim as select i, s from foo;