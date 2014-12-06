<?php
if ( !isset($_REQUEST['term']) )
{      
    echo 'error';
    exit;
}

include('./httpful.phar');

// EDIT these to match your schema
$tableName="Table";
$columnFamily="info";

// trim leading and trailing spaces
$searchTerm = strtolower(trim( $_REQUEST['term'] ));

// Connect to HBASE server
//  note: we are running in non "security" mode, so no user auth needed

$url = "http://localhost:8080/".$tableName."/".urlencode($searchTerm);

//$url = "http://localhost:8080/Table/".urlencode($searchTerm);
//Send Request
$response = \Httpful\Request::get($url)->addHeader('Accept','application/json')->send();

//DEBUG
//echo $response;

// iterate through response, adding each to array
$data = array();
$json = json_decode($response,true);
$row=$json["Row"];
$Cell=$row["Cell"];

if (!empty($Cell)){
   // if (array_key_exists("@column", $Cell)){
   //     $word = str_replace("$columnFamily:","",base64_decode($Cell["@column"]));
   //     $data[] array(
   //                  'label'=>$word,
   //                  'value'=>$searchTerm,"".$word
   //     );
   // } else{
    $hashset = array();

foreach($Cell as $item) {

    //DEBUG
    //echo $cell;

    //$word = str_replace("$columnFamily:","",base64_decode($item["@column"]));

    //DEBUG - Adding word probablility
    //echo $word;

    $column = base64_decode($item["@column"]);
    //$probability = base64_decode($item['$']);

    //$cellResponse = \Httpful\Request::get($url)->addHeader('Accept', 'application/json')->send();
     $url = "http://localhost:8080/" . $tableName . "/" . urlencode($searchTerm);
     $decodedResponse = json_decode($response, true);
     //$word = base64_decode($decodedResponse["Row"]["Cell"]["$"]);
     $word = str_replace("$columnFamily:","",base64_decode($item["@column"]));
     $data[] = array(
		    'label' => $word,
                     //'Prob" => intval($probability),
                    'value' => $searchTerm." ".$word
    );
        //$hashset[$word] = "ohpyeah";
    }
    //usort($data, 'sortByOrder');
//}
}
// return JSON encoded array to the JQuery autocomplete function
echo json_encode($data);
flush();
?>

