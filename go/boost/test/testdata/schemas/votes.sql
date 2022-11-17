CREATE TABLE article (id bigint NOT NULL AUTO_INCREMENT, title varchar(255), PRIMARY KEY(id));
CREATE TABLE vote (id bigint NOT NULL AUTO_INCREMENT, article_id bigint, user bigint, PRIMARY KEY(id));

/* DML */

SELECT /*vt+ VIEW=articlewithvotecount PUBLIC */ article.id, article.title, votecount.votes AS votes
FROM article
    LEFT JOIN (SELECT vote.article_id, COUNT(vote.user) AS votes
               FROM vote GROUP BY vote.article_id) AS votecount
    ON (article.id = votecount.article_id) WHERE article.id = :article_id;
