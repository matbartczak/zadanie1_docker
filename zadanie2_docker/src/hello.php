<?php
$dsn = "pgsql:host=db;dbname=" . getenv('PHP_DB_NAME');
$user = getenv('PHP_DB_USER');
$pass = getenv('PHP_DB_PASS');

try {
    $db = new PDO($dsn, $user, $pass);
    echo "Hello, world! skrypt php.<br>";
    echo "Użytkowniku: <br>" . $user;
} catch (PDOException $e) {
    echo "Błąd: " . $e->getMessage();
}