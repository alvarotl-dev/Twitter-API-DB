<?php
require_once('TwitterAPIExchange.php');
require_once('../PostgreSQLDB.php');
require_once('../MongoDB.php');
require_once('../SolrDB.php');

$twitter;

class twitter_php
{
    public $twitter;
    public $username;
    public $feed;
    public $pg;
    public $mongo;
    public $solr;

    public function  __construct()
    {
        //PostgreSQL
        $this->pg = new pg_php();
        //Mongo
        $this->mongo = new mongo_php();
        //Solr
        $this->solr = new solr_php();
    }

    public function initializeDatabases()
    {
        $this->pg->create_tables();
        $this->mongo->initialize_databases();
    }
    
    public function setConfig($username)
    {
        /** Set access tokens here - see: https://dev.twitter.com/apps/ **/
        $settings = array(
        'oauth_access_token' => "Insert Token Here",
        'oauth_access_token_secret' => "Insert Token Here",
        'consumer_key' => "Insert Token Here",
        'consumer_secret' => "Insert Token Here"
        );
        
        $url = "https://api.twitter.com/1.1/statuses/user_timeline.json";
        $requestMethod = "GET";
        
        if (isset($_GET['user']))  {$user = preg_replace("/[^A-Za-z0-9_]/", '', $_GET['user']);}  
        else {$user  = $username;}
        
        if (isset($_GET['count']) && is_numeric($_GET['count'])) 
        {
            $count =$_GET['count'];
        }
        else {$count = 10;}
        
        $getfield = "?screen_name=$user&count=$count&tweet_mode=extended";
        $this->twitter = new TwitterAPIExchange($settings);
        $this->feed = json_decode($this->twitter->setGetfield($getfield)
        ->buildOauth($url, $requestMethod)
        ->performRequest(),$assoc = TRUE);
        if(array_key_exists("errors", $this->feed)) {echo "<h3>Sorry, there was a problem.</h3><p>Twitter returned the following error message:</p><p><em>".$this->feed[errors][0]["message"]."</em></p>";exit();}
    }
    
    public function insertData()
    {
        $userAlready = false;
        $solrData = array();
        foreach($this->feed as $items)
        {    
            //****************//
            //GETTING THE DATA//
            //****************//

            $tweet = array($items['id_str'],$items['user']['screen_name'],$items['full_text'],$items['retweet_count'],$items['favorite_count'],$items['created_at']);
            
            $hashtags = array();
            $solr_hashtags = array();

            foreach($items['entities']['hashtags'] as $h)
            {
                $hashtag = array($items['id_str'],$h['text']);
                array_push($hashtags, $hashtag);
                $solr_h = ['id_str' => $hashtag[0],'text' => $hashtag[1]];
                array_push($solr_hashtags, $solr_h);
            }

            if(!$userAlready){
                $user_twitter = array($items['user']['screen_name'], $items['user']['name'], $items['user']['friends_count'], $items['user']['followers_count'], $items['user']['description'], $items['user']['location']);
                
                //PostgreSQL
                $this->pg->insert_user($user_twitter);
                
                //MongoDB
                $this->mongo->insert_user($user_twitter);

                //Solr
                $solr_user = $this->solr->convert_to_string($user_twitter);
                $solr_user = ['username' => $solr_user[0], 'name' => $solr_user[1], 'following' => $solr_user[2],
                'followers' => $solr_user[3], 'biography' => $solr_user[4]];
                array_push($solrData, $solr_user);

                $userAlready = true;
                
            }

            
            //****************//
            //*INSERTING DATA*//
            //****************//

            $this->pg->insert_tweet($tweet);    
            $this->mongo->insert_tweet($tweet,$hashtags);

            if(count($items['entities']['hashtags']) > 0)
            {
                foreach($hashtags as $h)
                {
                    $this->pg->insert_hashtag($hashtag);
                    array_push($solrData, $this->solr->convert_to_string($hashtag));
                }

                array_push($solrData, $solr_hashtags);
            }

            $solr_tweet = $this->solr->convert_to_string($tweet);
            $solr_tweet = ['tweet_id' => $solr_tweet[0], 'author' => $solr_tweet[1], 'content' => $solr_tweet[2],
            'rt_count' => $solr_tweet[3], 'fav_count' => $solr_tweet[4], 'date' => $solr_tweet[5]];

            array_push($solrData, $solr_tweet);
        }

        $this->solr->insert_data($solrData);
    }
}

$twitter = new twitter_php();

if(!isset($_GET['data']))
{ 
    $twitter->initializeDatabases();
    $twitter->setConfig("jack");
    $twitter->insertData();
    $twitter->setConfig("Twitter");
    $twitter->insertData();
    $twitter->setConfig("realmadrid");
    $twitter->insertData();
}
    
if(!isset($_GET['isquery']))
{

    echo "
    <html>
        <body>
            <h1> POSTGRESQL </h1>
            <h3><a href=?data=true&isquery=true&pg_query=simple>      Simple Query </a></h3>
            <h3><a href=?data=true&isquery=true&pg_query=join>    Join Query </a></h3>
            <h3><a href=?data=true&isquery=true&pg_query=aggregate>       Aggregate Query </a></h3>
            <h3><a href=?data=true&isquery=true&pg_query=mapreduce>       Map Reduce Query </a></h3>
            <h3><a href=?data=true&isquery=true&pg_query=search>      Text Search Query </a></h3>

            <br>
            

            <h1> MONGODB </h1>
            <h3><a href=?data=true&isquery=true&mongo_query=simple>      Simple Query </a></h3>
            <h3><a href=?data=true&isquery=true&mongo_query=join>    Join Query </a></h3>
            <h3><a href=?data=true&isquery=true&mongo_query=aggregate>       Aggregate Query </a></h3>
            <h3><a href=?data=true&isquery=true&mongo_query=mapreduce>       Map Reduce Query </a></h3>
            <h3><a href=?data=true&isquery=true&mongo_query=search>      Text Search Query </a></h3>
            
            <br>

            <h1> SOLR </h1>
            <h3><a href=?data=true&isquery=true&solr_query=simple>      Simple Query </a></h3>
            <h3><del>Join Query </del></h3>
            <h3><del>Aggregate Query </del></h3>
            <h3><del>Map Reduce Query </del></h3>
            <h3><a href=?data=true&isquery=true&solr_query=search>      Text Search Query </a></h3>
            


        </body>
    </html>";    
}

function GoBack()
{
    echo " 
        <html>
            <body>
                <h3><a href=?data=true> <<<< GO BACK </a></h3>
            </body>
        ";
}

if(isset($_GET['pg_query']))
{
    GoBack();
    
    if($_GET['pg_query'] == "simple")
        $twitter->pg->simple_query_examples();

    else if($_GET['pg_query'] == "join")
        $twitter->pg->join_query_examples();

    else if($_GET['pg_query'] == "aggregate")
        $twitter->pg->aggregate_query_examples();

    else if($_GET['pg_query'] == "mapreduce")
        $twitter->pg->map_reduce_query_examples();

    else if($_GET['pg_query'] == "search")
        $twitter->pg->text_search_query_examples("RT");

}



if(isset($_GET['mongo_query']))
{
    GoBack();

    if($_GET['mongo_query'] == "simple")
        $twitter->mongo->simple_query_examples();

    else if($_GET['mongo_query'] == "join")
        $twitter->mongo->join_query_examples();

    else if($_GET['mongo_query'] == "aggregate")
        $twitter->mongo->aggregate_query_examples();

    else if($_GET['mongo_query'] == "mapreduce")
        $twitter->mongo->map_reduce_query_examples();
    
    else if($_GET['mongo_query'] == "search")
        $twitter->mongo->text_search_query_examples("Minneapolis");

}

if(isset($_GET['solr_query']))
{   
    GoBack();

    if($_GET['solr_query'] == "simple")
        $twitter->solr->simple_query_examples();
    

    else if($_GET['solr_query'] == "search")
        $twitter->solr->text_search_query_examples("jack");

}


?>