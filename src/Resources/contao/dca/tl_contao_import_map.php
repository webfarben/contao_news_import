<?php

declare(strict_types=1);

$GLOBALS['TL_DCA']['tl_contao_import_map'] = [
    'config' => [
        'dataContainer' => 'Table',
        'sql' => [
            'keys' => [
                'id' => 'primary',
                'source_table,source_id,target_table' => 'unique',
            ],
        ],
    ],
    'fields' => [
        'id' => [
            'sql' => "int(10) unsigned NOT NULL auto_increment",
        ],
        'source_table' => [
            'sql' => "varchar(64) NOT NULL default ''",
        ],
        'source_id' => [
            'sql' => "int(10) unsigned NOT NULL default 0",
        ],
        'target_table' => [
            'sql' => "varchar(64) NOT NULL default ''",
        ],
        'target_id' => [
            'sql' => "int(10) unsigned NOT NULL default 0",
        ],
        'row_hash' => [
            'sql' => "char(64) NOT NULL default ''",
        ],
        'tstamp' => [
            'sql' => "int(10) unsigned NOT NULL default 0",
        ],
    ],
];
