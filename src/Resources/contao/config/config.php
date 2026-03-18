<?php

declare(strict_types=1);

$GLOBALS['BE_MOD']['system']['contao_news_import'] = [
    'callback' => Sebastian\ContaoImport\ContaoBackend\NewsImportBackendModule::class,
];
