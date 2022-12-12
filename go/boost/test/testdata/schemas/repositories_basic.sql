CREATE TABLE repositories (
	id bigint NOT NULL AUTO_INCREMENT,
	name varchar(255),
	url varchar(255),
	created_at datetime(6) NOT NULL,
	updated_at datetime(6) NOT NULL,
	PRIMARY KEY (id),
	KEY index_repositories_on_created_at (created_at)
) ENGINE InnoDB,
  CHARSET utf8mb4,
  COLLATE utf8mb4_0900_ai_ci;
CREATE TABLE repository_tags (
	id bigint NOT NULL AUTO_INCREMENT,
	tag_id bigint NOT NULL,
	repository_id bigint NOT NULL,
	created_at datetime(6) NOT NULL,
	updated_at datetime(6) NOT NULL,
	PRIMARY KEY (id),
	KEY index_repository_tags_on_tag_id (tag_id),
	KEY index_repository_tags_on_repository_id (repository_id)
) ENGINE InnoDB,
  CHARSET utf8mb4,
  COLLATE utf8mb4_0900_ai_ci;
CREATE TABLE stars (
	id bigint NOT NULL AUTO_INCREMENT,
	user_id bigint NOT NULL,
	repository_id bigint NOT NULL,
	spammy tinyint(1) NOT NULL DEFAULT '0',
	created_at datetime(6) NOT NULL,
	updated_at datetime(6) NOT NULL,
	PRIMARY KEY (id),
	KEY index_stars_on_user_id (user_id),
	KEY index_stars_on_repository_id (repository_id),
	KEY index_stars_on_spammy (spammy)
) ENGINE InnoDB,
  CHARSET utf8mb4,
  COLLATE utf8mb4_0900_ai_ci;
CREATE TABLE tags (
	id bigint NOT NULL AUTO_INCREMENT,
	name varchar(255),
	created_at datetime(6) NOT NULL,
	updated_at datetime(6) NOT NULL,
	PRIMARY KEY (id),
	UNIQUE KEY index_tags_on_name (name)
) ENGINE InnoDB,
  CHARSET utf8mb4,
  COLLATE utf8mb4_0900_ai_ci;