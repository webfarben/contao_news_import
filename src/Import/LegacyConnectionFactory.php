<?php

declare(strict_types=1);

namespace Sebastian\ContaoImport\Import;

use Doctrine\DBAL\Connection;
use Doctrine\DBAL\DriverManager;
use Doctrine\DBAL\Tools\DsnParser;

class LegacyConnectionFactory
{
    /**
     * @var array<string, Connection>
     */
    private array $connections = [];

    public function __construct(private readonly ?string $databaseUrl)
    {
    }

    public function getConnection(?string $databaseUrlOverride = null): Connection
    {
        $databaseUrl = $databaseUrlOverride ?? $this->databaseUrl;

        if (null === $databaseUrl || '' === trim($databaseUrl)) {
            throw new \RuntimeException('LEGACY_DATABASE_URL ist nicht gesetzt.');
        }

        $cacheKey = hash('sha256', $databaseUrl);
        if (isset($this->connections[$cacheKey])) {
            return $this->connections[$cacheKey];
        }

        $this->connections[$cacheKey] = DriverManager::getConnection($this->parseConnectionParams($databaseUrl));

        return $this->connections[$cacheKey];
    }

    /**
     * @return array<string, mixed>
     */
    private function parseConnectionParams(string $databaseUrl): array
    {
        $dsnParser = new DsnParser([
            'mysql' => 'pdo_mysql',
        ]);

        return $dsnParser->parse($databaseUrl);
    }
}
