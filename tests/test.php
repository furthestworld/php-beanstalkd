<?php

$beans = new beanstalkd();
$beans_config = [
    "host" => '127.0.0.1',
    "port" => 11300
];
$res = $beans->connect($beans_config['host'], $beans_config['port']);
var_dump($res);