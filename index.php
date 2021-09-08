<?php
	$endpoint = $_GET['a'];
	$response = shell_exec("python3 main.py -sa ${endpoint} 2>&1");
	$response = json_decode($response, true);
	http_response_code($response['status']);
	echo json_encode($response['data']);
	header("Content-type: application/json");
?>
