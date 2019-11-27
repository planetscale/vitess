-- MySQL dump 10.13  Distrib 5.7.26, for linux-glibc2.12 (x86_64)
--
-- Host: localhost    Database: _vt
-- ------------------------------------------------------
-- Server version	5.7.26-log

/*!40101 SET @OLD_CHARACTER_SET_CLIENT=@@CHARACTER_SET_CLIENT */;
/*!40101 SET @OLD_CHARACTER_SET_RESULTS=@@CHARACTER_SET_RESULTS */;
/*!40101 SET @OLD_COLLATION_CONNECTION=@@COLLATION_CONNECTION */;
/*!40101 SET NAMES utf8 */;
/*!40103 SET @OLD_TIME_ZONE=@@TIME_ZONE */;
/*!40103 SET TIME_ZONE='+00:00' */;
/*!40014 SET @OLD_UNIQUE_CHECKS=@@UNIQUE_CHECKS, UNIQUE_CHECKS=0 */;
/*!40014 SET @OLD_FOREIGN_KEY_CHECKS=@@FOREIGN_KEY_CHECKS, FOREIGN_KEY_CHECKS=0 */;
/*!40101 SET @OLD_SQL_MODE=@@SQL_MODE, SQL_MODE='NO_AUTO_VALUE_ON_ZERO' */;
/*!40111 SET @OLD_SQL_NOTES=@@SQL_NOTES, SQL_NOTES=0 */;

--
-- GTID state at the beginning of the backup 
--
set sql_log_bin=0;
--
-- Table structure for table `local_metadata`
--

DROP TABLE IF EXISTS `local_metadata`;
/*!40101 SET @saved_cs_client     = @@character_set_client */;
/*!40101 SET character_set_client = utf8 */;
CREATE TABLE `local_metadata` (
  `name` varchar(255) NOT NULL,
  `value` varchar(255) NOT NULL,
  `db_name` varbinary(255) NOT NULL,
  PRIMARY KEY (`db_name`,`name`)
) ENGINE=InnoDB DEFAULT CHARSET=latin1;
/*!40101 SET character_set_client = @saved_cs_client */;

--
-- Table structure for table `reparent_journal`
--

DROP TABLE IF EXISTS `reparent_journal`;
/*!40101 SET @saved_cs_client     = @@character_set_client */;
/*!40101 SET character_set_client = utf8 */;
CREATE TABLE `reparent_journal` (
  `time_created_ns` bigint(20) unsigned NOT NULL,
  `action_name` varbinary(250) NOT NULL,
  `master_alias` varbinary(32) NOT NULL,
  `replication_position` varbinary(64000) DEFAULT NULL,
  PRIMARY KEY (`time_created_ns`)
) ENGINE=InnoDB DEFAULT CHARSET=latin1;
/*!40101 SET character_set_client = @saved_cs_client */;

--
-- Table structure for table `resharding_journal`
--

DROP TABLE IF EXISTS `resharding_journal`;
/*!40101 SET @saved_cs_client     = @@character_set_client */;
/*!40101 SET character_set_client = utf8 */;
CREATE TABLE `resharding_journal` (
  `id` bigint(20) NOT NULL,
  `db_name` varbinary(255) DEFAULT NULL,
  `val` blob,
  PRIMARY KEY (`id`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8;
/*!40101 SET character_set_client = @saved_cs_client */;

--
-- Table structure for table `shard_metadata`
--

DROP TABLE IF EXISTS `shard_metadata`;
/*!40101 SET @saved_cs_client     = @@character_set_client */;
/*!40101 SET character_set_client = utf8 */;
CREATE TABLE `shard_metadata` (
  `name` varchar(255) NOT NULL,
  `value` mediumblob NOT NULL,
  `db_name` varbinary(255) NOT NULL,
  PRIMARY KEY (`db_name`,`name`)
) ENGINE=InnoDB DEFAULT CHARSET=latin1;
/*!40101 SET character_set_client = @saved_cs_client */;

--
-- Table structure for table `vreplication`
--

DROP TABLE IF EXISTS `vreplication`;
/*!40101 SET @saved_cs_client     = @@character_set_client */;
/*!40101 SET character_set_client = utf8 */;
CREATE TABLE `vreplication` (
  `id` int(11) NOT NULL AUTO_INCREMENT,
  `workflow` varbinary(1000) DEFAULT NULL,
  `source` varbinary(10000) NOT NULL,
  `pos` varbinary(10000) NOT NULL,
  `stop_pos` varbinary(10000) DEFAULT NULL,
  `max_tps` bigint(20) NOT NULL,
  `max_replication_lag` bigint(20) NOT NULL,
  `cell` varbinary(1000) DEFAULT NULL,
  `tablet_types` varbinary(100) DEFAULT NULL,
  `time_updated` bigint(20) NOT NULL,
  `transaction_timestamp` bigint(20) NOT NULL,
  `state` varbinary(100) NOT NULL,
  `message` varbinary(1000) DEFAULT NULL,
  `db_name` varbinary(255) NOT NULL,
  PRIMARY KEY (`id`)
) ENGINE=InnoDB AUTO_INCREMENT=2 DEFAULT CHARSET=utf8;
/*!40101 SET character_set_client = @saved_cs_client */;
/*!40103 SET TIME_ZONE=@OLD_TIME_ZONE */;

/*!40101 SET SQL_MODE=@OLD_SQL_MODE */;
/*!40014 SET FOREIGN_KEY_CHECKS=@OLD_FOREIGN_KEY_CHECKS */;
/*!40014 SET UNIQUE_CHECKS=@OLD_UNIQUE_CHECKS */;
/*!40101 SET CHARACTER_SET_CLIENT=@OLD_CHARACTER_SET_CLIENT */;
/*!40101 SET CHARACTER_SET_RESULTS=@OLD_CHARACTER_SET_RESULTS */;
/*!40101 SET COLLATION_CONNECTION=@OLD_COLLATION_CONNECTION */;
/*!40111 SET SQL_NOTES=@OLD_SQL_NOTES */;

-- Dump completed on 2019-11-16 19:38:20

