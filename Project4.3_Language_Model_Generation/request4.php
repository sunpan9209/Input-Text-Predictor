<?php
if ( !isset($_REQUEST['term']) )
{       echo 'error';
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

//Send Request
$response = \Httpful\Request::get($url)->addHeader('Accept','application/json')->send();

//DEBUG
//echo $response;

// iterate through response, adding each to array
$data = array();
$json = json_decode($response,true);
$row=$json["Row"];
$Cell=$row["Cell"];

$count = 0;
$tmparray = array();
$tmpwords = array();
foreach($Cell as $item) {
    //DEBUG
    //echo $cell;
    //echo base64_decode($item[0]);
    $word = str_replace("$columnFamily:","",base64_decode($item["@column"]));
    $probability = base64_decode($item['$']);
    $tmparray[$count] = $probability;
    //echo $probability;
    $tmpwords[$count] = $word;
    //echo $word;
    $count++;

    //DEBUG - Adding word probablility
    //echo $word;
    //$probability = base64_decode($item['$']);
}

for($x=0;$x<$count;$x++) {
    if ($count == 1)
        continue;
    for($y=$x+1;$y<$count;$y++) {
        if ($tmparray[$y] > $tmparray[$x]) {
            $tmp = $tmparray[$y];
            $tmparray[$y] = $tmparray[$x];
            $tmparray[$x] = $tmp;
	    $tmp = $tmpwords[$y];
            $tmpwords[$y] = $tmpwords[$x];
            $tmpwords[$x] = $tmp;
        }
        if ($tmparray[$y] == $tmparray[$x]){
             //echo tmpwords[$x];
           if (strcmp($tmpwords[$x],$tmpwords[$y])>0){
                $tmp = $tmpwords[$x];
                $tmpwords[$x] = $tmpwords[$y];
                $tmpwords[$y] = $tmp;
            }
        }
    }
}
//print "$tmpwords[0]";
$out = array();
$t = 0;
for($i=0;$i<$count;$i++) {
    if ($count == 1){
        $data[] = array(
		'label' => $tmpwords[0],//.", Prob:".$probability,
                'value' => $searchTerm." ".$tmpwords[0]
			);
        echo json_encode($data);
        echo json_encode($searchTerm);
    }
    else if (!in_array($tmpwords[$i], $out)){
        $data[] = array(
	    'label' => $tmpwords[$i],//.", Prob:".$probability,
            'value' => $searchTerm." ".$tmpwords[$i]
	);
        $out[$t++] = $tmpwords[$i];
    }
}
//echo "2 flag";
// return JSON encoded array to the JQuery autocomplete function
echo json_encode($data);
//print "3 flag";
flush();
?>


