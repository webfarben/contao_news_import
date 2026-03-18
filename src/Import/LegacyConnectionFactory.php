<?php

declare(strict_types=1);

namespace Sebastian\ContaoImport\Import;

use Doctrine\DBAL\Connection;
use Doctrine\DBAL\DriverManager;

class LegacyConnectionFactory
{
    private ?Connection $connection = null;

    public function __construct(private readonly ?string $databaseUrl)
    {
    }

    public function getConnection(): Connection
    {
        if (null !== $this->connection) {
            return $this->connection;
        }

        if (null === $this->databaseUrl || '' === trim($this->databaseUrl)) {
            throw new \RuntimeException('LEGACY_DATABASE_URL ist nicht gesetzt.');
        }

        $this->connection = DriverManager::getConnection([
            'url' => $this->databaseUrl,
        ]);

        return $this->connection;
    }
}
