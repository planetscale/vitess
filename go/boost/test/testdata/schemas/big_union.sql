CREATE TABLE `label`
(
    `id`         int unsigned                                                  NOT NULL AUTO_INCREMENT,
    `agency_id`  int unsigned                                                  NOT NULL,
    `name`       varchar(255) CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci NOT NULL,
    `slug`       varchar(255) CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci          DEFAULT NULL,
    `sort_order` int                                                                    DEFAULT NULL,
    `archived`   tinyint(1)                                                             DEFAULT NULL,
    `color`      varchar(255) CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci          DEFAULT NULL,
    `created_at` timestamp                                                     NOT NULL DEFAULT '0000-00-00 00:00:00',
    `updated_at` timestamp                                                     NOT NULL DEFAULT '0000-00-00 00:00:00',
    `deleted_at` timestamp                                                     NULL     DEFAULT NULL,
    PRIMARY KEY (`id`),
    KEY `label_agency_id_foreign` (`agency_id`)
) ENGINE = InnoDB;


CREATE TABLE `ticket_label`
(
    `id`         bigint unsigned NOT NULL AUTO_INCREMENT,
    `label_id`   int unsigned    NOT NULL,
    `ticket_id`  bigint unsigned NOT NULL,
    `created_at` timestamp       NOT NULL DEFAULT '1970-01-01 01:00:00',
    `updated_at` timestamp       NOT NULL DEFAULT '1970-01-01 01:00:00',
    PRIMARY KEY (`id`),
    KEY `ticket_label_ticket_id_foreign` (`ticket_id`),
    KEY `ticket_label_label_id_ticket_id_index` (`label_id`, `ticket_id`) USING BTREE
) ENGINE = InnoDB;

CREATE TABLE `ticket`
(
    `id`               bigint unsigned                                                                                       NOT NULL AUTO_INCREMENT,
    `agency_id`        int unsigned                                                                                          NOT NULL,
    `channel_id`       int unsigned                                                  DEFAULT NULL,
    `user_id`          int unsigned                                                  DEFAULT NULL,
    `team_id`          int unsigned                                                  DEFAULT NULL,
    `contact_id`       bigint unsigned                                               DEFAULT NULL,
    `assigned_by`      int unsigned                                                  DEFAULT NULL,
    `closed_by`        int unsigned                                                  DEFAULT NULL,
    `latest_message`   datetime                                                      DEFAULT NULL,
    `guid`             varchar(100) CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci DEFAULT NULL,
    `telegram_chat_id` varchar(30) CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci  DEFAULT NULL,
    `status`           enum ('URGENT','OPEN','ASSIGNED','CLOSED','INVALID') CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci NOT NULL,
    `subject`          varchar(191) CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci DEFAULT NULL,
    `closed_at`        datetime                                                      DEFAULT NULL,
    `assigned_at`      datetime                                                      DEFAULT NULL,
    `contact_cc`       text CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci,
    `custom_data`      text CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci,
    `created_at`       datetime                                                                                              NOT NULL,
    `updated_at`       datetime                                                      DEFAULT NULL,
    `deleted_at`       int                                                           DEFAULT NULL,
    PRIMARY KEY (`id`),
    KEY `ticket_agency_id_foreign` (`agency_id`),
    KEY `ticket_contact_id_foreign` (`contact_id`),
    KEY `ticket_assigned_by_foreign` (`assigned_by`),
    KEY `ticket_closed_by_foreign` (`closed_by`),
    KEY `ticket_status` (`status`),
    KEY `ticket_latest_message` (`latest_message`),
    KEY `ticket_guid` (`guid`),
    KEY `ticket_closed_at` (`closed_at`),
    KEY `ticket_team_id_foreign` (`team_id`),
    KEY `ticket_subject_index` (`subject`),
    KEY `ticket_agency_id_status_closed_at_index` (`agency_id`, `status`, `closed_at`),
    KEY `ticket_agency_id_status_latest_message_index` (`agency_id`, `status`, `latest_message`),
    KEY `created_at` (`created_at`),
    KEY `updated_at` (`updated_at`),
    KEY `ticket_agency_id_channel_id_subject_index` (`agency_id`, `channel_id`, `subject`),
    KEY `ticket_agency_id_status_latest_message_desc` (`agency_id`, `status`, `latest_message` DESC),
    KEY `ticket_user_id_status_index` (`user_id`, `status`) USING BTREE,
    KEY `ticket_channel_id_status_index` (`channel_id`, `status`) USING BTREE,
    KEY `ticket_team_id_deleted_at_status` (`team_id`, `deleted_at`, `status`) USING BTREE,
    KEY `ticket_agency_id_channel_id_status` (`agency_id`, `channel_id`, `status`) USING BTREE,
    KEY `ticket_agency_id_team_id_status` (`agency_id`, `team_id`, `status`),
    KEY `ticket_agency_id_user_id_status` (`agency_id`, `user_id`, `status`)
) ENGINE = InnoDB;

CREATE TABLE `ticket_mention`
(
    `id`         int unsigned    NOT NULL AUTO_INCREMENT,
    `agency_id`  int unsigned    NOT NULL,
    `ticket_id`  bigint unsigned NOT NULL,
    `message_id` bigint unsigned NOT NULL,
    `user_id`    int unsigned    NOT NULL,
    `seen`       tinyint(1)      NOT NULL DEFAULT '0',
    `created_at` timestamp       NULL     DEFAULT NULL,
    `updated_at` timestamp       NULL     DEFAULT NULL,
    PRIMARY KEY (`id`),
    KEY `ticket_mention_agency_id_foreign` (`agency_id`),
    KEY `ticket_mention_user_id_foreign` (`user_id`),
    KEY `ticket_mention_ticket_id_foreign` (`ticket_id`),
    KEY `ticket_mention_message_id_foreign` (`message_id`),
    KEY `ticket_mention_ticket_id_user_id_index` (`ticket_id`, `user_id`) USING BTREE
) ENGINE = InnoDB;

CREATE TABLE `user_channel_rel`
(
    `id`            bigint unsigned NOT NULL AUTO_INCREMENT,
    `user_id`       int unsigned    NOT NULL,
    `channel_id`    int unsigned    NOT NULL,
    `authorization` varchar(100) CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci DEFAULT NULL,
    `created_at`    timestamp       NULL                                          DEFAULT NULL,
    `updated_at`    timestamp       NULL                                          DEFAULT NULL,
    PRIMARY KEY (`id`),
    KEY `user_channel_rel_user_id_foreign` (`user_id`),
    KEY `user_channel_rel_channel_id_foreign` (`channel_id`),
    KEY `user_channel_rel_authorization_index` (`authorization`)
) ENGINE = InnoDB;

CREATE TABLE `team_user_rel`
(
    `id`                          int unsigned                                                                          NOT NULL AUTO_INCREMENT,
    `user_id`                     int unsigned                                                                          NOT NULL,
    `team_id`                     int unsigned                                                                          NOT NULL,
    `type`                        enum ('SUPERVISOR','MEMBER','OWNER') CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci NOT NULL,
    `created_at`                  timestamp                                                                             NOT NULL DEFAULT '1970-01-01 01:00:00',
    `updated_at`                  timestamp                                                                             NOT NULL DEFAULT '1970-01-01 01:00:00',
    `load_balancing_ticket_limit` tinyint unsigned                                                                      NOT NULL DEFAULT '0',
    PRIMARY KEY (`id`),
    KEY `team_user_rel_team_id_foreign` (`team_id`),
    KEY `user_id_team_id` (`user_id`, `team_id`)
) ENGINE = InnoDB;

CREATE TABLE `team_channel`
(
    `id`         int unsigned NOT NULL AUTO_INCREMENT,
    `team_id`    int unsigned NOT NULL,
    `channel_id` int unsigned NOT NULL,
    `created_at` timestamp    NOT NULL DEFAULT '1970-01-01 01:00:00',
    `updated_at` timestamp    NOT NULL DEFAULT '1970-01-01 01:00:00',
    PRIMARY KEY (`id`),
    KEY `team_channel_team_id_foreign` (`team_id`),
    KEY `team_channel_channel_id_foreign` (`channel_id`),
    KEY `team_id_channel_id` (`team_id`, `channel_id`)
) ENGINE = InnoDB;

select /*vt+ VIEW=c */
    label.id,
    ticket_label.ticket_id
from label
         join ticket_label on ticket_label.label_id = label.id
         join ticket ON ticket_label.ticket_id = ticket.id
WHERE ticket.agency_id = 12
  AND ticket.user_id = 15
  and ticket.`status` in (1,2,3)
UNION
select label.id,
       ticket_label.ticket_id
from label
         join ticket_label on ticket_label.label_id = label.id
         join ticket ON ticket_label.ticket_id = ticket.id
         join ticket_mention ON ticket_mention.ticket_id = ticket.id
WHERE ticket.agency_id = 12
  AND ticket_mention.user_id = 15
UNION
select label.id,
       ticket_label.ticket_id
from label
         join ticket_label on ticket_label.label_id = label.id
         join ticket ON ticket_label.ticket_id = ticket.id
         join user_channel_rel ON user_channel_rel.channel_id = ticket.channel_id
WHERE ticket.agency_id = 12
  AND user_channel_rel.user_id = 15
  and ticket.`status` in (1,2,3)
UNION
select label.id,
       ticket_label.ticket_id
from label
         join ticket_label on ticket_label.label_id = label.id
         join ticket FORCE INDEX (ticket_agency_id_team_id_status) ON ticket_label.ticket_id = ticket.id
         join team_user_rel ON team_user_rel.team_id = ticket.team_id
WHERE ticket.agency_id = 12
  and team_user_rel.user_id = 15
  and ticket.`status` in (1,2,3)
UNION
select label.id,
       ticket_label.ticket_id
from label
         join ticket_label on ticket_label.label_id = label.id
         join ticket FORCE INDEX (ticket_agency_id_channel_id_status) ON ticket_label.ticket_id = ticket.id
         join team_channel on team_channel.channel_id = ticket.channel_id
         join team_user_rel on team_user_rel.team_id = team_channel.team_id
WHERE ticket.agency_id = 12
  and team_user_rel.user_id = 15
  and ticket.`status` in (1,2,3);