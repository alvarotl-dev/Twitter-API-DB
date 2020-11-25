<?php

class queries
{
    public $pg_drop_query= '
        DROP TABLE IF EXISTS "user" CASCADE;
        DROP TABLE IF EXISTS "tweet" CASCADE;
        DROP TABLE IF EXISTS "hashtag" CASCADE;';

    public $pg_user_table = 'CREATE TABLE "user" (
        username    varchar(128) NOT NULL, 
        name        varchar(128),
        follows     integer NOT NULL,
        followers   integer NOT NULL, 
        biography   varchar(128), 
        location    varchar(128),
        PRIMARY KEY (username)
        );';

    public $pg_tweet_table = 'CREATE TABLE "tweet" (
        id          varchar(128) NOT NULL,
        author      varchar(128) NOT NULL,
        tw_content  varchar(280),
        rt_count    varchar(128),
        fav_count   varchar(128),
        date        varchar(128),
        PRIMARY KEY (id), 
        FOREIGN KEY (author) REFERENCES "user" (username)
        );';


    public $pg_hashtag_table = 'CREATE TABLE "hashtag" (
        id          SERIAL,
        tweet_id    varchar(128) NOT NULL,
        hashtag     varchar(280),
        PRIMARY KEY (id), 
        FOREIGN KEY (tweet_id) REFERENCES "tweet" (id)
        );';

    public function pg_user_insert($username, $name, $follows, $followers, $bio, $location)
    {
        $query = "INSERT INTO \"user\" 
        (username, name, follows, followers, biography, location)
        VALUES('$username', '$name', $follows, $followers, '$bio', '$location');";

        return $query; 
    }

    public function pg_tweet_insert($id, $author, $content, $rt_count, $fav_count, $date)
    {
        $query = "INSERT INTO \"tweet\" 
        (id, author, tw_content, rt_count, fav_count, date)
        VALUES($id, '$author', '$content', '$rt_count', '$fav_count','$date');";

        return $query; 
    }

    public function pg_hashtag_insert($tweet_id, $hashtag)
    {
        $query = "INSERT INTO \"hashtag\" 
        (tweet_id, hashtag)
        VALUES($tweet_id, '$hashtag');";

        return $query; 
    }

    public function pg_sq()
    {
        $query = "SELECT author, tw_content, rt_count, fav_count, date FROM \"tweet\"
            WHERE author = 'realmadrid'";

        return $query;
    }

    public function pg_join()
    {
        $query = "SELECT \"user\".username, name, follows, followers, \"tweet\".tw_content, \"tweet\".date 
            FROM \"user\"
            INNER JOIN \"tweet\" ON \"tweet\".author = \"user\".username;";

        return $query;
    }

    public function pg_aggregate()
    {
        $query = "SELECT author, max(CAST(rt_count AS INTEGER))
            FROM \"tweet\"
            GROUP BY author HAVING max(CAST(rt_count AS INTEGER))>1000;";

        return $query;
    }

    public function pg_map_reduce()
    {
        $query = "SELECT author, SUM(CAST(rt_count AS INTEGER))
            FROM \"tweet\"
            GROUP BY author;";

        return $query;
    }

    public function mongo_sq($select)
    {
        
        switch($select)
        {
            case 1:
                //Query by author in Tweet Collection
                $query = ['author' => 'realmadrid'];
                return $query;
                break;
                
            case 2:
                //Query by author in Tweet Collection
                $query = ['author' => 'jack'];
                return $query;
                break;

            case 3:
                //Query by username in User Collection
                $query = ['username' => 'Twitter'];
                return $query;
                break;
        }
    }

    public function mongo_aggregate($select)
    {
        switch($select)
        {
            case 1:
                //Pipeline for splitting the hashtags array.
                $pipeline = array(
                    array('$unwind' => '$hashtags') 
                    );
                return $pipeline;
                break;
                
            case 2:
                //Pipeline to search tweets from the collection user which are the same author.
                $pipeline = array(
                    array('$match' => ['author' =>'jack']) ,
                    array('$lookup'=>[
                        'from'=>'user',
                        'localField'=>'author',
                        'foreignField'=>'username',
                        'as'=>'user']));
                return $pipeline;
                break;
                
        }
    }

    public function mongo_map_reduce($select)
    {
        
        switch($select)
        {
            case 1:
                //Query that returns the total amount of RTs of the tweets from that author.
                $map = 'function(){emit(this.author, this.rt_count)}';
                $reduce = 'function(key, values){return Array.sum(values)}';
                $query = array($map, $reduce);
                return $query;
                break;        
        }
    }

    
    
    
};

$query_class = new queries();

?>