<?php

declare(strict_types=1);

namespace Sebastian\ContaoImport\ContaoManager;

use Contao\ManagerPlugin\Bundle\BundlePluginInterface;
use Contao\ManagerPlugin\Bundle\Config\BundleConfig;
use Contao\ManagerPlugin\Bundle\Parser\ParserInterface;
use Sebastian\ContaoImport\ContaoImportBundle;

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
