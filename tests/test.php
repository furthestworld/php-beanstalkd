<?php

$beans = new beanstalkd();
$beans_config1 = [
    "host" => '192.168.5.221',
    "port" => 11300
];
$beans_config2 = [
    "host" => '192.168.5.223',
    "port" => 11300
];
$res = $beans->connect($beans_config1['host'], $beans_config1['port']);
var_dump($res);
$res = $beans->addserver($beans_config2['host'], $beans_config2['port']);
var_dump($res);
