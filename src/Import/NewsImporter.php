<?php

declare(strict_types=1);

namespace Sebastian\ContaoImport\Import;

use Doctrine\DBAL\ArrayParameterType;
use Doctrine\DBAL\Connection;
use Doctrine\DBAL\Schema\Column;
use Doctrine\DBAL\Types\BigIntType;
use Doctrine\DBAL\Types\BooleanType;
use Doctrine\DBAL\Types\DecimalType;
use Doctrine\DBAL\Types\FloatType;
use Doctrine\DBAL\Types\IntegerType;
use Doctrine\DBAL\Types\SmallIntType;

class NewsImporter
{
    /**
     * @var array<string, array<string, string>>
     */
    private array $columnMap;

    /**
     * @var array<string, array<string, mixed>>
     */
    private array $fixedValues;

    /**
     * @var array<string, array<string, Column>>
     */
    private array $tableColumns = [];

    public function __construct(
        private readonly Connection $targetConnection,
        private readonly LegacyConnectionFactory $legacyConnectionFactory,
        array $columnMap = [],
        array $fixedValues = [],
    ) {
        $this->columnMap = $columnMap;
        $this->fixedValues = $fixedValues;
    }

    /**
     * @return array<string, array<string, int>>
     */
    public function import(ImportOptions $options): array
    {
        $legacy = $this->legacyConnectionFactory->getConnection($options->legacyDatabaseUrl);

        // MySQL DDL (CREATE TABLE) causes an implicit commit.
        // Ensure the map table exists before opening a transactional import.
        $this->ensureMapTable();

        if ($options->dryRun) {
            return $this->runImport($legacy, $options);
        }

        return $this->targetConnection->transactional(
            fn (Connection $_connection): array => $this->runImport($legacy, $options)
        );
    }

    /**
     * @return array<string, array<string, int>>
     */
    private function runImport(Connection $legacy, ImportOptions $options): array
    {
        $stats = [
            'tl_news_archive' => ['inserted' => 0, 'updated' => 0, 'skipped' => 0],
            'tl_news' => ['inserted' => 0, 'updated' => 0, 'skipped' => 0],
            'tl_content' => ['inserted' => 0, 'updated' => 0, 'skipped' => 0],
        ];

        if ($options->truncate) {
            if (!$options->dryRun) {
                $this->targetConnection->executeStatement("DELETE FROM tl_content WHERE ptable = 'tl_news'");
                $this->targetConnection->executeStatement('DELETE FROM tl_news');
                $this->targetConnection->executeStatement("DELETE FROM tl_contao_import_map WHERE target_table IN ('tl_news', 'tl_content')");

                if ($options->truncateArchives) {
                    $this->targetConnection->executeStatement('DELETE FROM tl_news_archive');
                    $this->targetConnection->executeStatement("DELETE FROM tl_contao_import_map WHERE target_table = 'tl_news_archive'");
                }
            }
        }

        $archiveWhere = null;
        $archiveParams = [];
        $archiveTypes = [];

        if ([] !== $options->archiveIds) {
            $archiveWhere = 'id IN (:archiveIds)';
            $archiveParams['archiveIds'] = $options->archiveIds;
            $archiveTypes['archiveIds'] = ArrayParameterType::INTEGER;
        }

        $stats['tl_news_archive'] = $this->syncTableById(
            $legacy,
            'tl_news_archive',
            $archiveWhere,
            $archiveParams,
            $archiveTypes,
            null,
            $options->dryRun,
            $options->truncate && !$options->truncateArchives
        );

        $newsWhereParts = ['1=1'];
        $newsParams = [];
        $newsTypes = [];

        if ([] !== $options->archiveIds) {
            $newsWhereParts[] = 'pid IN (:archiveIds)';
            $newsParams['archiveIds'] = $options->archiveIds;
            $newsTypes['archiveIds'] = ArrayParameterType::INTEGER;
        }

        if (null !== $options->since) {
            $newsWhereParts[] = 'date >= :since';
            $newsParams['since'] = $options->since;
        }

        if (null !== $options->until) {
            $newsWhereParts[] = 'date <= :until';
            $newsParams['until'] = $options->until;
        }

        $newsWhere = implode(' AND ', $newsWhereParts);

        $stats['tl_news'] = $this->syncTableById(
            $legacy,
            'tl_news',
            $newsWhere,
            $newsParams,
            $newsTypes,
            null,
            $options->dryRun,
            false
        );

        $legacyNewsIds = $this->fetchLegacyNewsIds($legacy, $newsWhere, $newsParams, $newsTypes);

        if ([] === $legacyNewsIds) {
            return $stats;
        }

        $stats['tl_content'] = $this->syncTableById(
            $legacy,
            'tl_content',
            'ptable = :ptable AND pid IN (:newsIds)',
            ['ptable' => 'tl_news', 'newsIds' => $legacyNewsIds],
            ['newsIds' => ArrayParameterType::INTEGER],
            static function (array $row): array {
                $row['ptable'] = 'tl_news';

                return $row;
            },
            $options->dryRun,
            false
        );

        return $stats;
    }

    /**
     * @param array<string, mixed> $params
     * @param callable(array<string, mixed>): array<string, mixed>|null $transform
     *
     * @return array<string, int>
     */
    private function syncTableById(
        Connection $legacy,
        string $table,
        ?string $where,
        array $params,
        array $types,
        ?callable $transform,
        bool $dryRun,
        bool $skipExistingInserts
    ): array {
        $rows = $this->fetchRows($legacy, $table, $where, $params, $types);

        $targetColumns = $this->getTargetColumns($table);
        $stats = ['inserted' => 0, 'updated' => 0, 'skipped' => 0];

        foreach ($rows as $rawRow) {
            if (!isset($rawRow['id'])) {
                ++$stats['skipped'];
                continue;
            }

            $sourceId = (int) $rawRow['id'];
            $row = array_filter(
                $transform ? $transform($rawRow) : $rawRow,
                static fn (mixed $value): bool => null !== $value
            );

            $row = $this->applyColumnMap($table, $row);
            $row = $this->applyFixedValues($table, $row);
            $row = $this->filterByColumns($row, $targetColumns);
            $row = $this->normalizeRowForTargetColumns($row, $targetColumns);

            if (!isset($row['id'])) {
                ++$stats['skipped'];
                continue;
            }

            $mapEntry = $this->findMapEntry($table, $sourceId, $table);
            $targetId = null !== $mapEntry ? (int) $mapEntry['target_id'] : (int) $row['id'];
            $row['id'] = $targetId;

            $hash = hash('sha256', json_encode($row, JSON_THROW_ON_ERROR));
            $exists = (bool) $this->targetConnection->fetchOne("SELECT 1 FROM {$table} WHERE id = ?", [$targetId]);

            if (null !== $mapEntry && $mapEntry['row_hash'] === $hash && $exists) {
                ++$stats['skipped'];
                continue;
            }

            if ($exists) {
                if ($skipExistingInserts) {
                    ++$stats['skipped'];
                    continue;
                }

                $updateData = $row;
                unset($updateData['id']);

                if (!$dryRun && [] !== $updateData) {
                    $this->targetConnection->update($table, $updateData, ['id' => $targetId]);
                    $this->upsertMapEntry($table, $sourceId, $table, $targetId, $hash);
                }

                ++$stats['updated'];
                continue;
            }

            if (!$dryRun) {
                $this->targetConnection->insert($table, $row);
                $this->upsertMapEntry($table, $sourceId, $table, $targetId, $hash);
            }

            ++$stats['inserted'];
        }

        return $stats;
    }

     /**
      * @param array<string, mixed> $row
      * @param array<string, Column> $targetColumns
      *
      * @return array<string, mixed>
      */
    private function filterByColumns(array $row, array $targetColumns): array
    {
        $filtered = [];

        foreach ($row as $column => $value) {
            if (isset($targetColumns[$column])) {
                $filtered[$column] = $value;
            }
        }

        return $filtered;
    }

    /**
     * @param array<string, mixed> $row
     * @param array<string, Column> $targetColumns
     *
     * @return array<string, mixed>
     */
    private function normalizeRowForTargetColumns(array $row, array $targetColumns): array
    {
        foreach ($row as $columnName => $value) {
            if ('' !== $value || !isset($targetColumns[$columnName])) {
                continue;
            }

            $column = $targetColumns[$columnName];
            $type = $column->getType();

            if (
                $type instanceof IntegerType
                || $type instanceof SmallIntType
                || $type instanceof BigIntType
                || $type instanceof BooleanType
                || $type instanceof FloatType
                || $type instanceof DecimalType
            ) {
                $default = $column->getDefault();

                if (null !== $default && '' !== (string) $default) {
                    $row[$columnName] = $default;
                    continue;
                }

                $row[$columnName] = $column->getNotnull() ? 0 : null;
                continue;
            }

            if (!$column->getNotnull()) {
                $row[$columnName] = null;
            }
        }

        return $row;
    }

    /**
     * @param array<string, mixed> $params
     * @param array<string, mixed> $types
     *
     * @return list<array<string, mixed>>
     */
    private function fetchRows(Connection $legacy, string $table, ?string $where, array $params, array $types): array
    {
        $queryBuilder = $legacy->createQueryBuilder();
        $queryBuilder
            ->select('*')
            ->from($table)
            ->where($where ?? '1=1')
            ->orderBy('id', 'ASC');

        foreach ($params as $name => $value) {
            $queryBuilder->setParameter($name, $value, $types[$name] ?? null);
        }

        return $queryBuilder->executeQuery()->fetchAllAssociative();
    }

    /**
     * @param array<string, mixed> $params
     * @param array<string, mixed> $types
     *
     * @return list<int>
     */
    private function fetchLegacyNewsIds(Connection $legacy, string $where, array $params, array $types): array
    {
        $queryBuilder = $legacy->createQueryBuilder();
        $queryBuilder
            ->select('id')
            ->from('tl_news')
            ->where($where)
            ->orderBy('id', 'ASC');

        foreach ($params as $name => $value) {
            $queryBuilder->setParameter($name, $value, $types[$name] ?? null);
        }

        return array_map('intval', $queryBuilder->executeQuery()->fetchFirstColumn());
    }

    /**
     * @return array<string, Column>
     */
    private function getTargetColumns(string $table): array
    {
        if (isset($this->tableColumns[$table])) {
            return $this->tableColumns[$table];
        }

        $this->tableColumns[$table] = $this->targetConnection->createSchemaManager()->listTableColumns($table);

        return $this->tableColumns[$table];
    }

    /**
     * @param array<string, mixed> $row
     *
     * @return array<string, mixed>
     */
    private function applyColumnMap(string $table, array $row): array
    {
        $tableMap = $this->columnMap[$table] ?? [];

        foreach ($tableMap as $legacyColumn => $targetColumn) {
            if (!array_key_exists($legacyColumn, $row)) {
                continue;
            }

            if (!array_key_exists($targetColumn, $row)) {
                $row[$targetColumn] = $row[$legacyColumn];
            }

            unset($row[$legacyColumn]);
        }

        return $row;
    }

    /**
     * @param array<string, mixed> $row
     *
     * @return array<string, mixed>
     */
    private function applyFixedValues(string $table, array $row): array
    {
        $tableValues = $this->fixedValues[$table] ?? [];

        foreach ($tableValues as $column => $value) {
            $row[$column] = $value;
        }

        return $row;
    }

    private function ensureMapTable(): void
    {
        // Keep this DDL outside transactional import execution.
        $this->targetConnection->executeStatement(
            'CREATE TABLE IF NOT EXISTS tl_contao_import_map (
                id INT UNSIGNED AUTO_INCREMENT NOT NULL,
                source_table VARCHAR(64) NOT NULL,
                source_id INT UNSIGNED NOT NULL,
                target_table VARCHAR(64) NOT NULL,
                target_id INT UNSIGNED NOT NULL,
                row_hash CHAR(64) NOT NULL,
                tstamp INT UNSIGNED NOT NULL,
                UNIQUE INDEX source_target (source_table, source_id, target_table),
                PRIMARY KEY(id)
            ) DEFAULT CHARACTER SET utf8mb4 COLLATE `utf8mb4_unicode_ci` ENGINE = InnoDB'
        );
    }

    /**
     * @return array{target_id: int, row_hash: string}|null
     */
    private function findMapEntry(string $sourceTable, int $sourceId, string $targetTable): ?array
    {
        $row = $this->targetConnection->fetchAssociative(
            'SELECT target_id, row_hash FROM tl_contao_import_map WHERE source_table = ? AND source_id = ? AND target_table = ?',
            [$sourceTable, $sourceId, $targetTable]
        );

        if (false === $row) {
            return null;
        }

        return [
            'target_id' => (int) $row['target_id'],
            'row_hash' => (string) $row['row_hash'],
        ];
    }

    private function upsertMapEntry(string $sourceTable, int $sourceId, string $targetTable, int $targetId, string $rowHash): void
    {
        $now = time();

        $this->targetConnection->executeStatement(
            'INSERT INTO tl_contao_import_map (source_table, source_id, target_table, target_id, row_hash, tstamp)
             VALUES (:source_table, :source_id, :target_table, :target_id, :row_hash, :tstamp)
             ON DUPLICATE KEY UPDATE target_id = VALUES(target_id), row_hash = VALUES(row_hash), tstamp = VALUES(tstamp)',
            [
                'source_table' => $sourceTable,
                'source_id' => $sourceId,
                'target_table' => $targetTable,
                'target_id' => $targetId,
                'row_hash' => $rowHash,
                'tstamp' => $now,
            ]
        );
    }
}
