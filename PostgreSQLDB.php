<?php

require_once("Queries.php");

//**********************************************//
//** Declaration of the PostgreSQL PHP Class ***//
//**********************************************//
class pg_php
{
    private $postgre;
    private $queries;

    public function __construct()
    {
        $this->queries = new queries();
        $this->postgre = pg_connect("host=localhost dbname=twitter user=postgres password=admin") or die('No se ha podido conectar: '. pg_last_error());
    }
    
    public function create_tables()
    {
        pg_query($this->queries->pg_drop_query) or die('La consulta fallo: ' . pg_last_error());
        pg_query($this->queries->pg_user_table) or die('La consulta fallo: ' . pg_last_error());
        pg_query($this->queries->pg_tweet_table) or die('La consulta fallo: ' . pg_last_error());
        pg_query($this->queries->pg_hashtag_table) or die('La consulta fallo: ' . pg_last_error());
    }

    public function insert_user($content)
    {
        $user = $this->escape($content);
        $query = $this->queries->pg_user_insert($user[0],$user[1],$user[2],$user[3],$user[4],$user[5]);
        pg_query($query);
    }

    public function insert_tweet($content)
    {
        $tweet = $this->escape($content);
        $query = $this->queries->pg_tweet_insert($tweet[0],$tweet[1],$tweet[2], $tweet[3], $tweet[4], $tweet[5]);
        pg_query($query);
    }
    
    public function insert_hashtag($content)
    {
        $hashtag = $this->escape($content);
        $query = $this->queries->pg_hashtag_insert($hashtag[0],$hashtag[1]);
        pg_query($query);
    }
    
    public function escape($content)
    {
        $escaped_content = array();

        foreach($content as $el)
        {
            if(is_string($el))
                array_push($escaped_content, pg_escape_string($el));
            
            else
                array_push($escaped_content, $el);

        }

        return $escaped_content;
    }


    public function show_data($result)
    {
        while($array = pg_fetch_array($result, null, PGSQL_ASSOC))
        {
            foreach($array as $key => $value)
            {
                echo $key." => ".$value."<br>";
            }
            echo "<br><br>";
        }
    }



    public function simple_query_examples()
    {
        echo '<h3>PostgreSQL Simple Query searching only tweets from realmadrid: </h3><br>';
        $query = $this->queries->pg_sq();
        $result = pg_query($query) or die('La consulta fallo: ' . pg_last_error());
        $this->show_data($result);
    }

    public function join_query_examples()
    {
        echo '<h3>PostgreSQL Join Query getting tweets from users registered in the user collection and showing its data: </h3><br>';
        $query = $this->queries->pg_join();
        $result = pg_query($query) or die('La consulta fallo: ' . pg_last_error());
        $this->show_data($result);
    }

    public function aggregate_query_examples()
    {
        echo '<h3>PostgreSQL Aggregate Query searching for the tweet with most RT in each account: </h3><br>';
        $query = $this->queries->pg_aggregate();
        $result = pg_query($query) or die('La consulta fallo: ' . pg_last_error());
        $this->show_data($result);
    }

    public function map_reduce_query_examples()
    {
        echo '<h3>PostgreSQL Map Reduce getting the sum of RT from every user tweets: </h3><br>';
        $query = $this->queries->pg_map_reduce();
        $result = pg_query($query) or die('La consulta fallo: ' . pg_last_error());
        $this->show_data($result);
    }

    public function text_search_query_examples($text)
    {
        echo '<h3>PostgreSQL Text Query with text search: '.$text.'</h3><br>';
        $query = "SELECT * FROM \"tweet\" WHERE tw_content LIKE '%{$text}%'";
        $result = pg_query($query) or die('La consulta fallo: ' . pg_last_error());
        $this->show_data($result);
    }

    public function close()
    {
        pg_close($this->postgre);
    }
};

//**********************************************//
//** Initialization and execution of the code **//
//**********************************************//

?>