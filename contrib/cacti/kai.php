#!/usr/bin/php
<?

$host = $argv[1];
$port = $argv[2];
$output = "";

$memcache = new Memcache;
$memcache->connect($host, $port) or die ("Could not connect");
$status = $memcache->getStats();
//print_r($status);

$output = "curr_items:" . $status["curr_items"] . " " .
		  "bytes:" . $status["bytes"] . " " .
		  "cmd_get:" . $status["cmd_get"] . " " .
		  "cmd_set:" . $status["cmd_set"] . " " .
		  "bytes_read:" . $status["bytes_read"] . " " .
		  "bytes_write:" . $status["bytes_write"] . " " .
		  "time:" . $status["time"] . " " .
		  "uptime:" . $status["uptime"] . " " .
		  "version:" . $status["version"] . " " ;

print $output;
/*
 * Local variables:
 * tab-width: 4
 * c-basic-offset: 4
 * End:
 * vim600: noet sw=4 ts=4 sts=4 fdm=marker
 * vim<600: noet sw=4 ts=4 sts=4
 */

?>
