<?php

require_once("vendor/autoload.php");
require_once("Queries.php");

//No se pueden hacer Joins en Solr
//no se puede hacer aggregate solamente con un pipeline con filtros (explicar y no hace falta ejemplo)
//map reduce tampoco se puede porque no tiene la funcionalidad
//echo 'Solarium library version: ' . Solarium\Client::VERSION . ' - ';

//**********************************************//
//****** Declaration of the Solr DB Class ******//
//**********************************************//
class solr_php
{
    private $solr;
    private $cfg;

    public function __construct()
    {
        $this->cfg = array(
            "endpoint" => array(
                "localhost" => array(
                    "host"=>"127.0.0.1",
                    "port"=>"8983", 
                    "path"=>"/", 
                    "collection"=>"task3")));

        $this->solr = new Solarium\Client(new Solarium\Core\Client\Adapter\Curl(), 
        new Symfony\Component\EventDispatcher\EventDispatcher(), $this->cfg);
    }

    public function insert_data($content)
    {
        $update = $this->solr->createUpdate();
        $res = $this->solr->update($update);

        foreach($content as $array)
        {   
            $update_query = $this->solr->createUpdate();
            $doc = $update->createDocument();

            foreach($array as $field => $value)
            {
                $doc->$field = $value;
            }
            $update_query->addDocument($doc);
            $update_query->addCommit();
            $res = $this->solr->update($update_query);
        }
        
    }

    public function convert_to_string($array)
    {
        $newArray = array();

        foreach($array as $el)
        {
            array_push($newArray, strval($el));
        }

        return $newArray;
    }

    public function ping()
    {
        $ping = $this->solr->createPing();
        try{
            $this->solr->ping($ping);
            echo "Correct ping. </ br></ br>";
        }
        catch(Solarium\Exception $ex){
            echo "Failed ping. </ br></ br>";
        }
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
                echo $key.": ".$value."<br />";

            else{
                echo $key.": ";
                $this->recursive_data($value, $html);
            }
                
        }
    }

    public function simple_query_examples()
    {
        echo '<h3>Solr Simple Query searching for tweets with 100 or more RTs</h3><br>';
        $query = $this->solr->createSelect();
        $query->setQuery('rt_count:[100 TO *]');
        $query->setStart(2)->setRows(10);
        $query->addSort('rt_count', $query::SORT_DESC);
        $result = $this->solr->select($query);

        $this->show_data($result);

    }
 
    public function text_search_query_examples($text)
    {
        echo '<h3>Solr Text Search Query searching user: '.$text.' and showing their most relevant tweets</h3><br>';
        $query = $this->solr->createSelect();
        $query->setQuery('author :*'.$text.'*');
        $query->setStart(2)->setRows(20);
        $query->addSort('rt_count', $query::SORT_DESC);

        $result = $this->solr->select($query);
        $this->show_data($result);
    }
}



?>