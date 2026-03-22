<?php

declare(strict_types=1);

$GLOBALS['BE_MOD']['system']['contao_news_import'] = [
    'callback' => webfarben\ContaoImport\ContaoBackend\NewsImportBackendModule::class,
];
