CREATE TABLE friend (usera int, userb int, primary key(usera, userb));
CREATE TABLE album (a_id varchar(255), u_id int, public tinyint(1), primary key(a_id));
CREATE TABLE photo (p_id varchar(255), album varchar(255), primary key(p_id));

(SELECT /*vt+ VIEW=album_friends */ album.a_id AS aid, friend.userb AS uid
 FROM album JOIN friend ON (album.u_id = friend.usera)
 WHERE album.public = 0)
UNION ALL
(SELECT album.a_id AS aid, friend.usera AS uid
 FROM album JOIN friend ON (album.u_id = friend.userb)
 WHERE album.public = 0)
UNION ALL
(SELECT album.a_id AS aid, album.u_id AS uid
 FROM album
 WHERE album.public = 0);

SELECT /*vt+ VIEW=private_photos */ photo.p_id FROM photo JOIN (
    (SELECT album.a_id AS aid, friend.userb AS uid
     FROM album JOIN friend ON (album.u_id = friend.usera)
     WHERE album.public = 0)
    UNION ALL
    (SELECT album.a_id AS aid, friend.usera AS uid
     FROM album JOIN friend ON (album.u_id = friend.userb)
     WHERE album.public = 0)
    UNION ALL
    (SELECT album.a_id AS aid, album.u_id AS uid
     FROM album
     WHERE album.public = 0)
) as album_friends ON (photo.album = album_friends.aid) WHERE album_friends.uid = :friend_id AND photo.album = :album_id;

SELECT /*vt+ VIEW=public_photos */ photo.p_id FROM photo JOIN album ON (photo.album = album.a_id) WHERE album.public = 1 AND album.a_id = :album_id;
