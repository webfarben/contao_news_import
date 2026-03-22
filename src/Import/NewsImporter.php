<?php

namespace webfarben\ContaoImport\Import;

use Doctrine\DBAL\Connection;
use Doctrine\DBAL\Types\Types;
use Doctrine\DBAL\Types\IntegerType;
use Doctrine\DBAL\Types\SmallIntType;
use Doctrine\DBAL\Types\BigIntType;
use Doctrine\DBAL\Types\BooleanType;
use Doctrine\DBAL\Types\FloatType;
use Doctrine\DBAL\Types\DecimalType;
use Doctrine\DBAL\Schema\Column;
use Doctrine\DBAL\ArrayParameterType;
use webfarben\ContaoImport\Import\LegacyConnectionFactory;
use webfarben\ContaoImport\Import\ImportOptions;

/**
 * Importiert alle Einträge aus der alten tl_files-Tabelle direkt per DB-Connection.
 * Übernimmt die UUIDs, sofern die Datei im Zielsystem existiert und noch kein Eintrag vorhanden ist.
 *
 * @param Connection $legacyConnection Verbindung zur alten Datenbank
 * @param string $filesDir Zielverzeichnis (z.B. 'files/')
 * @param bool $dryRun Nur simulieren, keine Inserts
 * @return int Anzahl importierter Dateien
 */
class NewsImporter
{
    /**
     * Importiert alle Einträge aus der alten tl_files-Tabelle direkt per DB-Connection.
     * Übernimmt die UUIDs, sofern die Datei im Zielsystem existiert und noch kein Eintrag vorhanden ist.
     *
     * @param Connection $legacyConnection Verbindung zur alten Datenbank
     * @param string $filesDir Zielverzeichnis (z.B. 'files/' oder beliebiges User-Verzeichnis)
     * @param bool $dryRun Nur simulieren, keine Inserts
     * @return int Anzahl importierter Dateien
     */
    public function importLegacyFilesFromDb(Connection $legacyConnection, string $filesDir, bool $dryRun = false): int
    {
        $rows = $legacyConnection->fetchAllAssociative('SELECT * FROM tl_files');
        return $this->importTlFilesRows($rows, $filesDir, $dryRun, 'importLegacyFilesFromDb');
    }
    /**
     * Importiert alle Einträge aus einer alten tl_files (z.B. aus einem Array-Export) in die neue tl_files.
     * Übernimmt dabei die alte UUID, sofern der Pfad noch nicht existiert.
     *
     * @param array $oldFilesRows Array mit alten tl_files-Datensätzen (uuid, path, name, extension, hash, etc.)
     * @param string $filesDir Zielverzeichnis (z.B. 'files/')
     * @return int Anzahl importierter Dateien
     */
    /**
     * Importiert alle Einträge aus einer alten tl_files (z.B. aus einem Array-Export) in die neue tl_files.
     * Übernimmt dabei die alte UUID, sofern der Pfad noch nicht existiert.
     *
     * @param array $oldFilesRows Array mit alten tl_files-Datensätzen (uuid, path, name, extension, hash, etc.)
     * @param string $filesDir Zielverzeichnis (z.B. 'files/' oder beliebiges User-Verzeichnis)
     * @return int Anzahl importierter Dateien
     */
    public function importLegacyFilesWithUuid(array $oldFilesRows, string $filesDir): int
    {
        return $this->importTlFilesRows($oldFilesRows, $filesDir, false, 'importLegacyFilesWithUuid');
    }

    /**
     * Gemeinsame Logik für den Import von tl_files-Einträgen (aus DB oder Datei).
     * @param array $rows
     * @param string $filesDir
     * @param bool $dryRun
     * @param string $logPrefix
     * @return int
     */
    private function importTlFilesRows(array $rows, string $filesDir, bool $dryRun, string $logPrefix): int
    {
        $imported = 0;
        foreach ($rows as $row) {
            if (empty($row['path']) || empty($row['uuid'])) {
                continue;
            }
            $path = ltrim(preg_replace('#^files/#i', '', $row['path']), '/');
            $fullPath = rtrim($filesDir, '/').'/'.$path;
            error_log('[ContaoImport] Prüfe Datei: ' . $fullPath);
            if (!is_file($fullPath)) {
                $this->debugLog($logPrefix.': Datei nicht gefunden: '.$fullPath);
                error_log('[ContaoImport] Datei NICHT gefunden: ' . $fullPath);
                continue;
            }
            $exists = $this->targetConnection->fetchOne('SELECT uuid FROM tl_files WHERE path = ?', [$path]);
            if ($exists) {
                $this->debugLog($logPrefix.': Datei bereits vorhanden: '.$path);
                continue;
            }
            $uuid = $row['uuid'];
            if (strlen($uuid) !== 16) {
                $uuid = $this->uuidToBin($uuid);
            }
            if (!$dryRun) {
                $this->targetConnection->insert('tl_files', [
                    'uuid' => $uuid,
                    'pid' => 0,
                    'tstamp' => isset($row['tstamp']) ? (int)$row['tstamp'] : time(),
                    'type' => $row['type'] ?? 'file',
                    'path' => $path,
                    'extension' => $row['extension'] ?? pathinfo($path, PATHINFO_EXTENSION),
                    'found' => 1,
                    'hash' => $row['hash'] ?? md5_file($fullPath),
                    'name' => $row['name'] ?? pathinfo($path, PATHINFO_FILENAME),
                ]);
            }
            $this->debugLog($logPrefix.': importiert '.$path.' mit uuid='.bin2hex($uuid));
            ++$imported;
        }
        return $imported;
    }

/**
 * HINWEIS ZUM BILDER-IMPORT:
 *
 * Um die Bildreferenzen (UUIDs) aus der alten Contao-Installation korrekt zu übernehmen,
 * müssen die Dateien aus dem alten System nicht nur ins neue Filesystem (z.B. files/...) kopiert werden,
 * sondern auch die alten UUIDs (Spalte uuid aus alter tl_files) beim Import in die neue tl_files übernommen werden.
 *
 * Vorgehen:
 * 1. Exportiere die alte tl_files (mind. uuid, path, name, extension, hash, etc.).
 * 2. Beim Import/Synchronisieren prüfe:
 *    - Gibt es für den Pfad schon einen Eintrag? → ggf. abgleichen/überschreiben.
 *    - Wenn nicht, lege einen neuen Datensatz mit der alten uuid (als binär!) an:
 *      $this->handleFileReference($path, $filesDir, $alteUuid);
 * 3. Nur so bleiben die Referenzen in tl_news/tl_content gültig.
 *
 * Alternativ: Nachträgliches Mapping alter zu neuer UUIDs ist möglich, aber fehleranfällig.
 *
 * Siehe auch die Option $forceUuid in handleFileReference().
 */
    /**
     * Aktualisiert Bildreferenzen (singleSRC, multiSRC, enclosure) in News-Datensätzen.
     * Erwartet, dass die Dateien bereits im Zielverzeichnis liegen.
     * Legt ggf. neue UUIDs in tl_files an und aktualisiert die Referenzen.
     *
     * @param array $newsRows Referenz auf News-Datensätze
     * @param string $filesDir Zielverzeichnis (z.B. 'files/')
     */
    private function updateNewsImageReferences(array &$newsRows, string $filesDir): void
    {
        foreach ($newsRows as &$row) {
            // singleSRC (setze sowohl CamelCase als auch lower-case Schlüssel, damit filterByColumns passt)
            if (!empty($row['singleSRC']) || !empty($row['singlesrc'])) {
                $srcVal = !empty($row['singleSRC']) ? $row['singleSRC'] : $row['singlesrc'];
                $new = $this->handleFileReference($srcVal, $filesDir);
                if ('' !== $new) {
                    $row['singleSRC'] = $new;
                    $row['singlesrc'] = $new;
                    // addimage setzen
                    $row['addimage'] = 1;
                    $row['addImage'] = 1;
                } else {
                    // Keine Datei gefunden -> entferne Referenzen
                    unset($row['singleSRC'], $row['singlesrc']);
                }
            }
            // multiSRC (serialized UUIDs oder Pfade)
            if (!empty($row['multiSRC'])) {
                $multi = @unserialize($row['multiSRC']);
                if (is_array($multi)) {
                    $newMulti = [];
                    foreach ($multi as $src) {
                        $val = $this->handleFileReference($src, $filesDir);
                        if ('' !== $val) {
                            $newMulti[] = $val;
                        }
                    }
                    if ([] !== $newMulti) {
                        $row['multiSRC'] = serialize($newMulti);
                        $row['multisrc'] = $row['multiSRC'];
                        // Debug-Ausgabe: Hex-UUIDs loggen
                        $this->debugLog('updateNewsImageReferences: multiSRC converted to ' . json_encode(array_map(fn($u) => bin2hex($u), $newMulti)));
                    } else {
                        unset($row['multiSRC'], $row['multisrc']);
                    }
                }
            }
            // enclosure (analog zu multiSRC)
            if (!empty($row['enclosure'])) {
                $encl = @unserialize($row['enclosure']);
                if (is_array($encl)) {
                    $newEncl = [];
                    foreach ($encl as $src) {
                        $val = $this->handleFileReference($src, $filesDir);
                        if ('' !== $val) {
                            $newEncl[] = $val;
                        }
                    }
                    if ([] !== $newEncl) {
                        $row['enclosure'] = serialize($newEncl);
                        $this->debugLog('updateNewsImageReferences: enclosure converted to ' . json_encode(array_map(fn($u) => bin2hex($u), $newEncl)));
                    } else {
                        unset($row['enclosure']);
                    }
                }
            }
        }
    }

    /**
     * Prüft, ob eine Datei bereits in tl_files existiert, legt ggf. einen Eintrag an und gibt die UUID (binär) zurück.
     * Erwartet, dass die Datei im Zielverzeichnis liegt.
     *
     * @param string $src UUID (binär oder String) oder Dateipfad
     * @param string $filesDir Zielverzeichnis
     * @return string UUID (binär) für die Referenz in tl_news
     */
    /**
     * @param string $src UUID (binär oder String) oder Dateipfad
     * @param string $filesDir Zielverzeichnis
     * @param string|null $forceUuid UUID (String oder binär), die beim Insert verwendet werden soll
     * @return string UUID (binär) für die Referenz in tl_news
     */
    private function handleFileReference(string $src, string $filesDir, $forceUuid = null): string
    {
        // Debug: eingehenden Wert protokollieren (binär -> hex für Lesbarkeit)
        $logVal = (1 === preg_match('//u', $src) ? $src : bin2hex($src));
        $this->debugLog(sprintf('handleFileReference: input=%s', $logVal));

        // Normalisieren nur für lesbare Textwerte; bei Binärdaten (16 Byte UUID) nichts ändern
        $isPrintable = 1 === preg_match('/^[\x20-\x7E]+$/', $src);
        if ($isPrintable) {
            $src = trim($src);
            $src = preg_replace('#^files/#i', '', $src);
            $src = ltrim($src, '/');
        }
        $logNorm = $isPrintable ? $src : bin2hex($src);
        $this->debugLog(sprintf('handleFileReference: normalized=%s', $logNorm));

        // Wenn bereits 16-Byte Binärstring übergeben wurde, direkt zurückgeben
        if (strlen($src) === 16 && 1 !== preg_match('/^[0-9a-fA-F]+$/', $src)) {
            $this->debugLog('handleFileReference: detected 16-byte binary input, returning as-is');
            return $src;
        }

        // Erkenne 32-hex (hex string ohne bindestriche)
        if (1 === preg_match('/^[0-9a-f]{32}$/i', $src)) {
            $bin = @hex2bin($src);
            if (false !== $bin && 16 === strlen($bin)) {
                $this->debugLog('handleFileReference: detected 32-hex, converted to binary');
                $exists = $this->targetConnection->fetchOne('SELECT uuid FROM tl_files WHERE uuid = ?', [$bin]);
                if ($exists) {
                    $this->debugLog('handleFileReference: uuid found in tl_files');
                    return $bin;
                }
                // Wenn nicht gefunden: wir behandeln weiter unten (ggf. Insert ohne Pfad)
                return $bin;
            }
        }

        // Erkenne 36-char UUID mit Bindestrichen
        if (1 === preg_match('/^[a-f0-9\-]{36}$/i', $src)) {
            $bin = $this->uuidToBin($src);
            $this->debugLog('handleFileReference: detected 36-char UUID, converted to binary');
            $exists = $this->targetConnection->fetchOne('SELECT uuid FROM tl_files WHERE uuid = ?', [$bin]);
            if ($exists) {
                $this->debugLog('handleFileReference: uuid found in tl_files');
                return $bin;
            }
            // UUID bekannt aber nicht in tl_files -> wir geben das Binär zurück (oder legen neuen Eintrag an, falls Pfad existiert)
        }

        // Andernfalls: behandeln als Pfad
        $path = $src;
        $fullPath = rtrim($filesDir, '/').'/'.$path;
        $this->debugLog(sprintf('handleFileReference: checking file path=%s exists=%s', $path, is_file($fullPath) ? 'yes' : 'no'));

        if (!is_file($fullPath)) {
            $this->debugLog('handleFileReference: file not found on disk, returning empty');
            return '';
        }

        // Prüfen, ob Datei schon in tl_files ist (pfad-basiert)
        $row = $this->targetConnection->fetchAssociative('SELECT uuid FROM tl_files WHERE path = ?', [$path]);
        if ($row && !empty($row['uuid'])) {
            $this->debugLog('handleFileReference: found existing tl_files entry for path');
            return $row['uuid'];
        }

        // Ansonsten neuen Eintrag anlegen
        if ($forceUuid !== null) {
            // forceUuid kann binär oder String sein
            if (strlen($forceUuid) === 16 && 1 !== preg_match('/^[0-9a-fA-F]+$/', $forceUuid)) {
                $bin = $forceUuid;
                $uuid = vsprintf('%s%s-%s-%s-%s-%s%s%s', str_split(bin2hex($bin), 4));
            } else {
                $uuid = str_replace(['{','}'], '', $forceUuid);
                $bin = $this->uuidToBin($uuid);
            }
        } else {
            $uuid = $this->generateUuid();
            $bin = $this->uuidToBin($uuid);
        }
        $this->debugLog(sprintf('handleFileReference: creating new tl_files entry uuid=%s path=%s', $uuid, $path));

        $this->targetConnection->insert('tl_files', [
            'uuid' => $bin,
            'pid' => 0,
            'tstamp' => time(),
            'type' => 'file',
            'path' => $path,
            'extension' => pathinfo($path, PATHINFO_EXTENSION),
            'found' => 1,
            'hash' => md5_file($fullPath),
            'name' => pathinfo($path, PATHINFO_FILENAME),
        ]);

        $this->debugLog(sprintf('handleFileReference: inserted tl_files uuid=%s (hex=%s)', $uuid, bin2hex($bin)));

        return $bin;
    }

    /**
     * Generiert eine neue UUID (String, 36 Zeichen)
     */
    private function generateUuid(): string
    {
        $data = random_bytes(16);
        $data[6] = chr((ord($data[6]) & 0x0f) | 0x40); // Version 4
        $data[8] = chr((ord($data[8]) & 0x3f) | 0x80); // Variant
        return vsprintf('%s%s-%s-%s-%s-%s%s%s', str_split(bin2hex($data), 4));
    }

    /**
     * Wandelt eine UUID (String) in Binär (16 Byte) um
     */
    private function uuidToBin(string $uuid): string
    {
        return hex2bin(str_replace('-', '', $uuid));
    }
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


        // News-Datensätze laden
        $newsRows = $this->fetchRows($legacy, 'tl_news', $newsWhere, $newsParams, $newsTypes);
        // Bildreferenzen aktualisieren (Dateipfad ggf. anpassen)
        $this->updateNewsImageReferences($newsRows, $options->filesDir ?? 'files/');
        // News-Datensätze schreiben
        $stats['tl_news'] =  ['inserted' => 0, 'updated' => 0, 'skipped' => 0];
        foreach ($newsRows as $row) {
            // syncTableById-Logik für einzelne Zeile (vereinfacht, da Mapping/Update/Insert-Logik schon vorhanden)
            $row = $this->applyColumnMap('tl_news', $row);
            $row = $this->applyFixedValues('tl_news', $row);
            // addImage / addimage automatisch setzen, wenn singleSRC gefüllt ist
            if (!empty($row['singleSRC']) || !empty($row['singlesrc'])) {
                $row['addImage'] = 1;
                $row['addimage'] = 1;
            }
            $row = $this->filterByColumns($row, $this->getTargetColumns('tl_news'));
            $row = $this->validateStringLengths($row, $this->getTargetColumns('tl_news'), 'tl_news');
            $row = $this->normalizeRowForTargetColumns($row, $this->getTargetColumns('tl_news'));
            $row = $this->normalizeRowEncoding($row);
            $this->logColumnLengths('tl_news', $row, $this->getTargetColumns('tl_news'));
            if (!isset($row['id'])) {
                ++$stats['tl_news']['skipped'];
                continue;
            }
            $mapEntry = $this->findMapEntry('tl_news', (int)$row['id'], 'tl_news');
            $targetId = null !== $mapEntry ? (int)$mapEntry['target_id'] : (int)$row['id'];
            $row['id'] = $targetId;
            $hash = hash('sha256', json_encode($row, JSON_THROW_ON_ERROR | JSON_INVALID_UTF8_SUBSTITUTE));
            // Debug: log row and target columns to help diagnose missing image fields
            $this->debugLog('tl_news prepare: targetColumns=' . implode(',', array_keys($this->getTargetColumns('tl_news'))) . ' row=' . json_encode($this->sanitizeRowForLog($row)));
            $exists = (bool)$this->targetConnection->fetchOne("SELECT 1 FROM tl_news WHERE id = ?", [$targetId]);
            if (null !== $mapEntry && $mapEntry['row_hash'] === $hash && $exists) {
                ++$stats['tl_news']['skipped'];
                continue;
            }
            if ($exists) {
                $updateData = $row;
                unset($updateData['id']);
                if (!$options->dryRun && [] !== $updateData) {
                    $this->targetConnection->update('tl_news', $updateData, ['id' => $targetId]);
                    $this->upsertMapEntry('tl_news', (int)$row['id'], 'tl_news', $targetId, $hash);
                }
                ++$stats['tl_news']['updated'];
                continue;
            }
            if (!$options->dryRun) {
                $this->targetConnection->insert('tl_news', $row);
                $this->upsertMapEntry('tl_news', (int)$row['id'], 'tl_news', $targetId, $hash);
                $this->debugLog('tl_news inserted id=' . $targetId . ' row=' . json_encode($this->sanitizeRowForLog($row)));
            }
            ++$stats['tl_news']['inserted'];
        }

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
        // Wenn die Tabelle Datei-Referenzen (singleSRC/multiSRC) enthält, versuche diese zu konvertieren.
        // Das stellt sicher, dass z.B. tl_content.singleSRC vor dem Schreiben in binary(16) umgewandelt wird.
        if (array_key_exists('singleSRC', $targetColumns) || array_key_exists('singlesrc', $targetColumns) || array_key_exists('multiSRC', $targetColumns) || array_key_exists('multisrc', $targetColumns)) {
            // ACHTUNG: filesDir muss von außen übergeben werden!
            // Diese Methode sollte nur noch über runImport() mit korrekt gesetztem filesDir aufgerufen werden.
            $this->updateNewsImageReferences($rows, $options->filesDir ?? 'files/');
        }
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
            $row = $this->validateStringLengths($row, $targetColumns, $table);
            $row = $this->normalizeRowForTargetColumns($row, $targetColumns);
            $row = $this->normalizeRowEncoding($row);
            $this->logColumnLengths($table, $row, $targetColumns);

            if (!isset($row['id'])) {
                ++$stats['skipped'];
                continue;
            }

            $mapEntry = $this->findMapEntry($table, $sourceId, $table);
            $targetId = null !== $mapEntry ? (int) $mapEntry['target_id'] : (int) $row['id'];
            $row['id'] = $targetId;

            $hash = hash('sha256', json_encode($row, JSON_THROW_ON_ERROR | JSON_INVALID_UTF8_SUBSTITUTE));
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

        // Build a lowercase -> actual column name map for case-insensitive matching
        $lowerMap = [];
        foreach (array_keys($targetColumns) as $col) {
            $lowerMap[strtolower($col)] = $col;
        }

        foreach ($row as $column => $value) {
            $lower = strtolower($column);

            if (isset($lowerMap[$lower])) {
                $actual = $lowerMap[$lower];
                $filtered[$actual] = $value;
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
     * @param array<string, mixed> $row
     *
     * @return array<string, mixed>
     */
    private function normalizeRowEncoding(array $row): array
    {
        foreach ($row as $columnName => $value) {
            if (!\is_string($value) || '' === $value) {
                continue;
            }

            // Wenn der Wert 16 Byte lang ist und kein gültiger UTF-8-String,
            // handelt es sich sehr wahrscheinlich um eine binäre UUID (binary(16)).
            // Solche Werte dürfen nicht per Encoding-Konvertierung verändert werden.
            if (16 === strlen($value) && 1 !== preg_match('//u', $value)) {
                continue;
            }

            if (1 === preg_match('//u', $value)) {
                continue;
            }

            $row[$columnName] = $this->toUtf8($value);
        }

        return $row;
    }

    private function toUtf8(string $value): string
    {
        if (\function_exists('mb_convert_encoding')) {
            $converted = mb_convert_encoding($value, 'UTF-8', 'UTF-8,ISO-8859-1,Windows-1252');

            if (\is_string($converted) && 1 === preg_match('//u', $converted)) {
                return $converted;
            }
        }

        if (\function_exists('iconv')) {
            $converted = iconv('ISO-8859-1', 'UTF-8//IGNORE', $value);

            if (false !== $converted && 1 === preg_match('//u', $converted)) {
                return $converted;
            }

            $converted = iconv('UTF-8', 'UTF-8//IGNORE', $value);

            if (false !== $converted && 1 === preg_match('//u', $converted)) {
                return $converted;
            }
        }

        return preg_replace('/[^\x09\x0A\x0D\x20-\x7E]/', '', $value) ?? '';
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

        $this->bindQueryParameters($queryBuilder, $params, $types);

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

        $this->bindQueryParameters($queryBuilder, $params, $types);

        return array_map('intval', $queryBuilder->executeQuery()->fetchFirstColumn());
    }

    /**
     * @param array<string, mixed> $params
     * @param array<string, mixed> $types
     */
    private function bindQueryParameters(\Doctrine\DBAL\Query\QueryBuilder $queryBuilder, array $params, array $types): void
    {
        foreach ($params as $name => $value) {
            if (array_key_exists($name, $types)) {
                $queryBuilder->setParameter($name, $value, $types[$name]);
                continue;
            }

            $queryBuilder->setParameter($name, $value);
        }
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

    /**
     * Schreibe Debug-Meldung in Logdatei (anhängend).
     */
    private function debugLog(string $message): void
    {
        $logDir = __DIR__ . '/../../var/log';
        if (!is_dir($logDir)) {
            @mkdir($logDir, 0775, true);
        }
        $file = $logDir . '/contao_import_debug.log';
        $line = date('c') . ' ' . $message . PHP_EOL;
        @file_put_contents($file, $line, FILE_APPEND | LOCK_EX);
    }

    /**
     * Sanitisiert ein Array für Logging (wandelt nicht-utf8 Strings in hex um).
     *
     * @param array<string,mixed> $row
     * @return array<string,mixed>
     */
    private function sanitizeRowForLog(array $row): array
    {
        $out = [];
        foreach ($row as $k => $v) {
            if (is_string($v)) {
                if (1 !== preg_match('//u', $v)) {
                    $out[$k] = bin2hex($v);
                    continue;
                }
            }
            if (is_array($v)) {
                $out[$k] = $this->sanitizeRowForLog($v);
                continue;
            }
            $out[$k] = $v;
        }

        return $out;
    }

    /**
     * Entfernt string-Werte, die länger sind als die Zielsäulen-Länge und loggt Warnungen.
     *
     * @param array<string,mixed> $row
     * @param array<string, Column> $targetColumns
     * @return array<string,mixed>
     */
    private function validateStringLengths(array $row, array $targetColumns, string $table): array
    {
        foreach ($row as $col => $val) {
            if (!is_string($val)) {
                continue;
            }

            if (!isset($targetColumns[$col])) {
                continue;
            }

            $column = $targetColumns[$col];
            $length = $column->getLength();

            if (null === $length) {
                continue;
            }

            // byte-length check to match DB storage
            $len = strlen($val);
            if ($len > $length) {
                // If target is binary(16) and value looks like 32 hex chars or 36-char UUID, convert to 16-byte binary
                if (16 === $length) {
                    if (1 === preg_match('/^[0-9a-f]{32}$/i', $val)) {
                        $bin = @hex2bin($val);
                        if (false !== $bin && 16 === strlen($bin)) {
                            $row[$col] = $bin;
                            continue;
                        }
                    }

                    if (1 === preg_match('/^[a-f0-9\-]{36}$/i', $val)) {
                        $bin = $this->uuidToBin($val);
                        if (false !== $bin && 16 === strlen($bin)) {
                            $row[$col] = $bin;
                            continue;
                        }
                    }
                }

                $this->debugLog(sprintf('%s: value for column %s too long (%d > %d) — removing value to avoid SQL error', $table, $col, $len, $length));
                unset($row[$col]);
            }
        }

        return $row;
    }

    /**
     * Loggt die Byte-Längen string-ähnlicher Werte gegenüber der Zielsäulen-Länge.
     * Hilft beim Auffinden, welche Spalte den SQL1406 auslöst.
     *
     * @param string $table
     * @param array<string,mixed> $row
     * @param array<string, Column> $targetColumns
     */
    private function logColumnLengths(string $table, array $row, array $targetColumns): void
    {
        $parts = [];

        foreach ($row as $col => $val) {
            if (!is_string($val)) {
                continue;
            }

            $len = strlen($val);
            $targetLen = $targetColumns[$col]->getLength() ?? null;

            $parts[] = sprintf('%s=%d', $col, $len) . ($targetLen ? sprintf('(%d)', $targetLen) : '');
        }

        if ([] !== $parts) {
            $this->debugLog(sprintf("%s: pre-write column lengths: %s", $table, implode(', ', $parts)));
        }
    }
}
