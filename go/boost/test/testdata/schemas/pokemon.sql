CREATE TABLE `Pokemon` (
   `id` int NOT NULL,
   `name` varchar(191) NOT NULL,
   `spriteUrl` varchar(191) NOT NULL,
   PRIMARY KEY (`id`)
);

CREATE TABLE `Vote` (
    `id` varchar(191) NOT NULL,
    `createdAt` datetime(3) NOT NULL DEFAULT CURRENT_TIMESTAMP(3),
    `votedForId` int NOT NULL,
    `votedAgainstId` int NOT NULL,
    PRIMARY KEY (`id`),
    KEY `Vote_votedForId_idx` (`votedForId`),
    KEY `Vote_votedAgainstId_idx` (`votedAgainstId`)
);

select /*vt+ VIEW=pokemon_votes */ Pokemon.id,
       Pokemon.`name`,
       Pokemon.spriteUrl,
       aggr_selection_0_Vote._aggr_count_voteFor,
       aggr_selection_1_Vote._aggr_count_voteAgainst
from Pokemon
         left join (select Vote.votedForId, count(*) as _aggr_count_voteFor
                    from Vote
                    where :vtg1 = :vtg1
                    group by Vote.votedForId) as aggr_selection_0_Vote
                   on Pokemon.id = aggr_selection_0_Vote.votedForId
         left join (select Vote.votedAgainstId, count(*) as _aggr_count_voteAgainst
                    from Vote
                    where :vtg1 = :vtg1
                    group by Vote.votedAgainstId) as aggr_selection_1_Vote
                   on Pokemon.id = aggr_selection_1_Vote.votedAgainstId
where :vtg1 = :vtg1;