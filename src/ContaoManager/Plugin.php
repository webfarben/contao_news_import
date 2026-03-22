<?php

declare(strict_types=1);

namespace webfarben\ContaoImport\ContaoManager;

use Contao\ManagerPlugin\Bundle\BundlePluginInterface;
use Contao\ManagerPlugin\Bundle\Config\BundleConfig;
use Contao\ManagerPlugin\Bundle\Parser\ParserInterface;
use webfarben\ContaoImport\ContaoImportBundle;

class Plugin implements BundlePluginInterface
{
    public function getBundles(ParserInterface $parser): array
    {
        return [
            BundleConfig::create(ContaoImportBundle::class)
                ->setLoadAfter(['Contao\\CoreBundle\\ContaoCoreBundle']),
        ];
    }
}
