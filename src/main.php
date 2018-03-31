<?php

require_once(__DIR__ . '/vendor/autoload.php');
require_once(__DIR__ . '/settings.php');


function getMasterVarcharDb()
{
    static $db = null;
    
    if ($db === null)
    {
        $db = new mysqli(DB_HOST, DB_USER, DB_PASSWORD, MASTER_VARCHAR_DB_NAME);
    }
    
    return $db;
}

function getMasterTextDb()
{
    static $db = null;
    
    if ($db === null)
    {
        $db = new mysqli(DB_HOST, DB_USER, DB_PASSWORD, MASTER_TEXT_DB_NAME);
    }
    
    return $db;
}


function getSlaveVarcharDb()
{
    static $db = null;
    
    if ($db === null)
    {
        $db = new mysqli(DB_HOST, DB_USER, DB_PASSWORD, SLAVE_VARCHAR_DB_NAME);
    }
    
    return $db;
}


function getSlaveTextDb()
{
    static $db = null;
    
    if ($db === null)
    {
        $db = new mysqli(DB_HOST, DB_USER, DB_PASSWORD, SLAVE_TEXT_DB_NAME);
    }
    
    return $db;
}


function initDb()
{
    createLogTables();
    insertMasterLogs();
    insertSlaveLog();
}


function createLogTables()
{
    $varcharQueries = array();
    $varcharQueries[] = "DROP TABLE IF EXISTS `logs_varchar`";
    
    $varcharQueries[] = 
        "CREATE TABLE `logs_varchar` (
            `id` int unsigned NOT NULL AUTO_INCREMENT,
            `message` varchar(6000) NOT NULL,
            `creation_time` int UNSIGNED NOT NULL,
            PRIMARY KEY (`id`)
        ) ENGINE=InnoDB DEFAULT CHARSET=utf8;";
    
    foreach (array(getMasterVarcharDb(), getSlaveVarcharDb()) as $db)
    {
        foreach ($varcharQueries as $query)
        {
            $db->query($query) or die("failed to execute query:\n{$query}\n Error: " . $db->error);
        }
    }
    
    $textfieldQueries = array();
    $textfieldQueries[] = "DROP TABLE IF EXISTS `logs_textfield`";
    
    $textfieldQueries[] = 
        "CREATE TABLE `logs_textfield` (
            `id` int unsigned NOT NULL AUTO_INCREMENT,
            `message` TEXT NOT NULL,
            `creation_time` int UNSIGNED NOT NULL,
            PRIMARY KEY (`id`)
        ) ENGINE=InnoDB DEFAULT CHARSET=utf8;";
    
    foreach (array(getMasterTextDb(), getSlaveTextDb()) as $db)
    {
        foreach ($textfieldQueries as $query)
        {
            $db->query($query) or die("failed to execute query:\n{$query}\n Error: " . $db->error);
        }
    }
}


/**
 * Insert the logs into the master database.
 * @throws Exception
 */
function insertMasterLogs()
{
    print "Inserting master logs..." . PHP_EOL;
    $logsToInsert = array();
    
    for ($i=1; $i<=NUM_LOGS; $i++)
    {
        $logsToInsert[] = [
            'id' => $i, 
            'message' => LOG_TEXT,
            'creation_time' => time(),
        ];
        
        if (($i % BATCH_SIZE) === 0)
        {
            // insert the same log into the two different tables.
            foreach (array('logs_textfield' => getMasterTextDb(), 'logs_varchar' => getMasterVarcharDb()) as $tableName => $db)
            {
                $batchInsertQuery = \iRAP\CoreLibs\MysqliLib::generateBatchInsertQuery(
                    $logsToInsert, 
                    $tableName, 
                    $db
                );

                // run the batch insert quries and time them to see how much impact the TEXT
                // type has.
                \iRAP\Profiling\FunctionAnalyzer::start('insert_' . $tableName);
                $result = $db->query($batchInsertQuery);
                \iRAP\Profiling\FunctionAnalyzer::stop('insert_' . $tableName);

                if ($result === FALSE)
                {
                    print "Failed to insert batch of logs." . PHP_EOL;
                    print $batchInsertQuery . PHP_EOL;
                    print "db error: " . $db->error . PHP_EOL;
                    die();
                }
            }
            
            $logsToInsert = array();
            
            // show progress bar because this could take a while.
            $percentageComplete = $i / NUM_LOGS * 100;
            iRAP\CoreLibs\CliLib::showProgressBar($percentageComplete, 0);
        }
    }
    
    // print a newline to get past the progress bar.
    print PHP_EOL;
}



/**
 * Insert the logs into the master database.
 * @throws Exception
 */
function insertSlaveLog()
{
    print "Inserting slave logs..." . PHP_EOL;
    $logsToInsert = array();
    
    for ($i=1; $i<=NUM_LOGS; $i++)
    {
        $logsToInsert[] = [
            'id' => $i, 
            'message' => 'hello world',
            'creation_time' => time(),
        ];
        
        if (($i % BATCH_SIZE) === 0)
        {
            // insert the same log into the two different tables.
            foreach (array('logs_textfield' => getSlaveTextDb(), 'logs_varchar' => getSlaveVarcharDb()) as $tableName => $db)
            {
                $batchInsertQuery = \iRAP\CoreLibs\MysqliLib::generateBatchInsertQuery(
                    $logsToInsert, 
                    $tableName, 
                    $db
                );

                // run the batch insert quries but don't bother timing them this time.
                $result = $db->query($batchInsertQuery);

                if ($result === FALSE)
                {
                    print "Failed to insert batch of logs." . PHP_EOL;
                    print $batchInsertQuery . PHP_EOL;
                    print "db error: " . $db->error . PHP_EOL;
                    die();
                }
            }
            
            $logsToInsert = array();
            
            // show progress bar because this could take a while.
            $percentageComplete = $i / NUM_LOGS * 100;
            iRAP\CoreLibs\CliLib::showProgressBar($percentageComplete, 0);
        }
    }
    
    // print a newline to get past the progress bar.
    print PHP_EOL;
}


/**
 * Perform a sync of the primary dataabse to the slave database and time how much faster it is
 * to sync the varchar based one compared to the text type.
 */
function performSync()
{
    $configs = array(
        'varchar' => array(
            'master' => MASTER_VARCHAR_DB_NAME,
            'slave' => SLAVE_VARCHAR_DB_NAME,
        ),
        'text' => array(
            'master' => MASTER_TEXT_DB_NAME,
            'slave' => SLAVE_TEXT_DB_NAME,
        ),
    );
    
    foreach ($configs as $testType => $dbPair)
    {
        $settingsContent = "<?php
            
            # CURRENT LIVE DATABASE
            define('MASTER_DB_NAME', '" . $dbPair['master'] . "');
            define('MASTER_DB_HOST', '" . DB_HOST . "');
            define('MASTER_DB_USER', '" . DB_USER . "');
            define('MASTER_DB_PASSWORD', '" . DB_PASSWORD . "');
            define('MASTER_DB_PORT', 3306);

            # local dev database
            define('SLAVE_DB_NAME', '" . $dbPair['slave'] . "');
            define('SLAVE_DB_HOST', '" . DB_HOST . "');
            define('SLAVE_DB_USER', '" . DB_USER . "');
            define('SLAVE_DB_PASSWORD', '" . DB_PASSWORD . "');
            define('SLAVE_DB_PORT', 3306);

            # sync database (for helping this tool). 
            define('SYNC_DB_NAME', '" . SYNC_DB_NAME . "');
            define('SYNC_DB_HOST', '" . DB_HOST . "');
            define('SYNC_DB_USER', '" . DB_USER . "');
            define('SYNC_DB_PASSWORD', '" . DB_PASSWORD . "');
            define('SYNC_DB_PORT', 3306);


            # Set this to true if you want to log the changes that take place.
            # e.g. all table drops/inserts/deletions etc.
            define('LOG_QUERIES', false);

            # specify the file location where we want to log queries
            define('LOG_QUERY_FILE', __DIR__ . '/queries.sql');

            # Define the number of rows we wish to sync at a time. This prevents issues arising from trying
            # to sync massive tables and running out of memory, or hitting the MAX_PACKET_SIZE limit on your db.
            define('CHUNK_SIZE', 100);


            # Set this to true if you want to allow java to run each of the commands in a thread pool.
            # This is optimal if you have multiple cores.
            define('USE_MULTI_PROCESSING', false);


            # Tables that do not have a primary key cannot be synced.
            # If this is set to true, then those tables will just be completely replaced (fully copy)
            # if set to false, this will just skip those tables.
            define('COPY_TABLES_WITH_NO_PRIMARY', true);


            # Specify regexp list of tables that we should ignore syncing. They won't be removed, updated, or
            # added to either database.
            define('IGNORE_TABLES', array());

            // One to one relationship of table name to the name of the column it uses as a 'partition' or grouping.
            // These tables will be synced in parts based on the unique values of the partition. E.g. if you have
            // 5 different partition values in your table, then that table will be synced in 5 stages.
            // we do not use regular expresssions here because having overlaps is too risky.
            define('PARTITIONED_TABLE_DEFINITIONS', array());";
        
        file_put_contents(
            __DIR__ . '/../tools/MySQL-Syncer-master/src/settings/settings.php',
            $settingsContent
        );
        
        // run and time the test
        print "Syncing $testType tables" . PHP_EOL;
        iRAP\Profiling\FunctionAnalyzer::start('sync_test_' . $testType);
        passthru('/usr/bin/php ' . __DIR__ . '/../tools/MySQL-Syncer-master/src/project/main.php');
        iRAP\Profiling\FunctionAnalyzer::stop('sync_test_' . $testType);
    }
}


function main()
{
    initDb();
    performSync();
    
    print iRAP\Profiling\FunctionAnalyzer::getResults();
}

main();