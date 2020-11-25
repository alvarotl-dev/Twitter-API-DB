<?php

require_once("vendor/autoload.php");
require_once("Queries.php");

//En mongo solo se pueden hacer joins con aggregates

//**********************************************//
//**** Declaration of the MongoDB PHP Class ****//
//**********************************************//
class mongo_php
{
    private $mongo;
    private $queries;
    private $m_db;          //Database
    private $m_col_tweet;   //Tweet Collection
    private $m_col_user;    //User Collection
    private $m_cur;         //Cursor

    public function __construct()
    {
        //CLOUD - $this->mongo = new MongoDB\Client('mongodb+srv://AbsydeAuberon:<password>@auberoncluster-ywak9.mongodb.net/test?authSource=admin&replicaSet=AuberonCluster-shard-0&readPreference=primary&appname=MongoDB%20Compass%20Community&ssl=true');
        //LOCALHOST - 
        $this->mongo = new MongoDB\Client('mongodb://localhost:27017/?readPreference=primary&appname=MongoDB%20Compass%20Community&ssl=false');
        $this->queries = new queries();
        $this->m_db = $this->mongo->twitter;    //Database name:   "twitter".
        $this->m_col_tweet = $this->m_db->tweet; //Collection name: "tweet".
        $this->m_col_user = $this->m_db->user;   //Collection name: "user".
        
    }

    public function initialize_databases()
    {
        $this->m_db = $this->mongo->twitter;
        $this->m_db->dropCollection("tweet");
        $this->m_db->dropCollection("user");
        $this->m_db->createCollection("tweet");
        $this->m_db->createCollection("user");
        $this->m_col_tweet = $this->m_db->tweet; //Collection name: "tweet".
        $this->m_col_user = $this->m_db->user;   //Collection name: "user".
        $this->m_col_tweet->createIndex(["$**" => "text"]); //Wildcard text index for the tweet collection in order to do text searches.
    }

    public function insert_user($user)
    {
        $this->m_col_user->insertOne(array(
            "username" => $user[0],
            "name" => $user[1],
            "follows" => $user[2],
            "followers" => $user[3],
            "biography" => $user[4],
            "location" => $user[5]));
    }

    public function insert_tweet($tweet, $hashtags)
    {
        $this->m_col_tweet->insertOne(array(
            "id" => $tweet[0],
            "author" => $tweet[1],
            "content" => $tweet[2],
            "rt_count" => $tweet[3],
            "fav_count" => $tweet[4],
            "date" => $tweet[5], 
            "hashtags" => $hashtags));
    }


    public function show_data($cursor)
    {
        $html = "";

        foreach ($cursor as $element)
        {
            $this->recursive_data($element, $html);   
            echo "<br /><br />";
        }
    }
    
    public function recursive_data($element, $html)
    {
        foreach($element as $key => $value)
        {
            if(!is_array($value))
                echo $key."=> ".$value."<br />";

            else{
                echo $key."=> <br />";
                $this->recursive_data($value, $html);
            }
                
        }
    }


    public function simple_query_examples()
    {
        echo '<h3>MongoDB Simple Query searching only tweets from jack: </h3><br>';
        $query = $this->queries->mongo_sq(2);   
        $cursor = $this->m_col_tweet->find($query);
        $cursor->setTypeMap(['root' => 'array', 'document' => 'array', 'array' => 'array']);
        $this->show_data($cursor);
    }


    public function join_query_examples()
    {
        echo '<h3>MongoDB Join Query using aggregation with $lookup searching tweets which have the same author and username from jack and joining the information: </h3><br>';
        $query = $this->queries->mongo_aggregate(2);
        $cursor = $this->m_col_tweet->aggregate($query);
        $cursor->setTypeMap(['root' => 'array', 'document' => 'array', 'array' => 'array']);
        $this->show_data($cursor);
    }

    public function aggregate_query_examples()
    {
        echo '<h3>MongoDB Aggregate Query searching only tweets with hashtags with $unwind: </h3><br>';
        $query = $this->queries->mongo_aggregate(1);
        $cursor = $this->m_col_tweet->aggregate($query);
        $cursor->setTypeMap(['root' => 'array', 'document' => 'array', 'array' => 'array']);
        $this->show_data($cursor);
    }

    public function map_reduce_query_examples()
    {
        echo '<h3>MongoDB Map Reduce Query displaying only the user and the rt_count amount in their tweets: </h3><br>';
        $query = $this->queries->mongo_map_reduce(1);
        
        $map = new MongoDB\BSON\Javascript($query[0]);
        $reduce = new MongoDB\BSON\Javascript($query[1]);
        $out = ['inline' => 1];

        $cursor = $this->m_col_tweet->mapReduce($map,$reduce,$out);
        $this->show_data($cursor);
       
    }

    public function text_search_query_examples($text)
    {
        echo '<h3>MongoDB text Query with text search: '.$text.'</h3><br>';
        $filter = ['$text' => ['$search' => $text]];
        $cursor =  $this->m_col_tweet->find($filter);
        
        $cursor->setTypeMap(['root' => 'array', 'document' => 'array', 'array' => 'array']);
        $this->show_data($cursor);
    
    }
}

?>