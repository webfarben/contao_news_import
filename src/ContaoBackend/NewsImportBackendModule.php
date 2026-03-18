<?php

declare(strict_types=1);

namespace Sebastian\ContaoImport\ContaoBackend;

use Contao\BackendModule;
use Contao\Config;
use Contao\Environment;
use Contao\Input;
use Contao\StringUtil;
use Contao\System;
use Sebastian\ContaoImport\Import\ImportOptions;
use Sebastian\ContaoImport\Import\NewsImporter;

class NewsImportBackendModule extends BackendModule
{
    protected $strTemplate = 'be_contao_news_import';

    protected function compile(): void
    {
        $formData = [
            'source_host' => $this->inputValue('source_host', (string) Config::get('contaoNewsImportSourceHost')),
            'source_port' => $this->inputValue('source_port', (string) Config::get('contaoNewsImportSourcePort')),
            'source_database' => $this->inputValue('source_database', (string) Config::get('contaoNewsImportSourceDatabase')),
            'source_user' => $this->inputValue('source_user', (string) Config::get('contaoNewsImportSourceUser')),
            'source_password' => $this->inputValue('source_password', (string) Config::get('contaoNewsImportSourcePassword')),
            'archive_ids' => $this->inputValue('archive_ids', ''),
            'since' => $this->inputValue('since', ''),
            'until' => $this->inputValue('until', ''),
            'dry_run' => '1' === Input::post('dry_run'),
            'truncate' => '1' === Input::post('truncate'),
            'truncate_archives' => '1' === Input::post('truncate_archives'),
            'save_credentials' => '1' === Input::post('save_credentials'),
        ];

        $this->Template->headline = 'Legacy-News-Import';
        $this->Template->action = StringUtil::ampersand(Environment::get('request'));
        $this->Template->requestToken = defined('REQUEST_TOKEN') ? REQUEST_TOKEN : '';
        $this->Template->statusMessage = null;
        $this->Template->statusType = null;
        $this->Template->stats = null;
        $this->Template->formData = $formData;

        if ('tl_contao_news_import' !== Input::post('FORM_SUBMIT')) {
            return;
        }
        
            // Log form submission
            \error_log('=== CONTAO NEWS IMPORT START ===');
            \error_log('DryRun: ' . ($dryRun ? 'yes' : 'no'));
            \error_log('URL: ' . ($legacyDatabaseUrl ?? 'null'));

        $dryRun = $formData['dry_run'];
        $truncate = $formData['truncate'];
        $truncateArchives = $formData['truncate_archives'];
        $saveCredentials = $formData['save_credentials'];

        $legacyDatabaseUrl = $this->buildLegacyDatabaseUrl(
            (string) $formData['source_host'],
            (string) $formData['source_port'],
            (string) $formData['source_database'],
            (string) $formData['source_user'],
            (string) $formData['source_password']
        );

        if (null === $legacyDatabaseUrl) {
            $this->Template->statusType = 'error';
            $this->Template->statusMessage = 'Bitte Host, Port, Datenbank und Benutzer fuer die Quelldatenbank korrekt eintragen.';
            $this->Template->formData = $formData;

            return;
        }

        // Test connection before proceeding
        // Handle test connection request
        if ('test_connection' === Input::post('action')) {
            try {
                $legacyConnectionFactory = System::getContainer()->get('Sebastian\ContaoImport\Import\LegacyConnectionFactory');
                $testConnection = $legacyConnectionFactory->getConnection($legacyDatabaseUrl);
                $testConnection->executeQuery('SELECT 1');
                
                $this->Template->statusType = 'success';
                $this->Template->statusMessage = '✓ Verbindung erfolgreich! Die Quelldatenbank ist erreichbar.';
            } catch (\Throwable $e) {
                $this->Template->statusType = 'error';
                $this->Template->statusMessage = 'FEHLER: ' . $e->getMessage();
            }
            $this->Template->formData = $formData;
            return;
        }

        // Test connection before proceeding with actual import
        try {
            $legacyConnectionFactory = System::getContainer()->get('Sebastian\ContaoImport\Import\LegacyConnectionFactory');
            $testConnection = $legacyConnectionFactory->getConnection($legacyDatabaseUrl);
            // Simple query to verify connection works
            $testConnection->executeQuery('SELECT 1');
        } catch (\Throwable $e) {
            $this->Template->statusType = 'error';
            $this->Template->statusMessage = 'FEHLER bei Verbindung: ' . $e->getMessage();
            $this->Template->formData = $formData;

            return;
        }

        if ($saveCredentials) {
            Config::persist('contaoNewsImportSourceHost', (string) $formData['source_host']);
            Config::persist('contaoNewsImportSourcePort', (string) $formData['source_port']);
            Config::persist('contaoNewsImportSourceDatabase', (string) $formData['source_database']);
            Config::persist('contaoNewsImportSourceUser', (string) $formData['source_user']);
            Config::persist('contaoNewsImportSourcePassword', (string) $formData['source_password']);
        }

        if ($truncateArchives && !$truncate) {
            $this->Template->statusType = 'error';
            $this->Template->statusMessage = 'Die Option "Archive loeschen" funktioniert nur zusammen mit "News/Inhalte leeren".';
            $this->Template->formData = $formData;

            return;
        }

        $archiveIds = $this->parseArchiveIds((string) $formData['archive_ids']);

        if (null === $archiveIds) {
            $this->Template->statusType = 'error';
            $this->Template->statusMessage = 'Archive-ID-Liste ist ungueltig. Bitte kommagetrennte Zahlen eintragen.';
            $this->Template->formData = $formData;

            return;
        }

        $since = $this->parseDateValue((string) $formData['since'], false);
        $until = $this->parseDateValue((string) $formData['until'], true);

        if (false === $since || false === $until) {
            $this->Template->statusType = 'error';
            $this->Template->statusMessage = 'Datumswerte muessen YYYY-MM-DD oder Unix-Timestamp sein.';
            $this->Template->formData = $formData;

            return;
        }

        if (null !== $since && null !== $until && $since > $until) {
            $this->Template->statusType = 'error';
            $this->Template->statusMessage = '"Seit" darf nicht groesser als "Bis" sein.';
            $this->Template->formData = $formData;

            return;
        }

        $options = new ImportOptions(
            dryRun: $dryRun,
            truncate: $truncate,
            truncateArchives: $truncateArchives,
            archiveIds: $archiveIds,
            since: $since,
            until: $until,
            legacyDatabaseUrl: $legacyDatabaseUrl,
        );

        try {
            /** @var NewsImporter $importer */
            $importer = System::getContainer()->get(NewsImporter::class);
            $stats = $importer->import($options);

            // Debug: Log stats structure
            \error_log('ImportStats: ' . json_encode($stats, JSON_PRETTY_PRINT));

            $this->Template->statusType = 'success';
            
            // Build detailed success message
            $totalInserted = (int) array_sum(array_column($stats, 'inserted'));
            $totalUpdated = (int) array_sum(array_column($stats, 'updated'));
            $totalSkipped = (int) array_sum(array_column($stats, 'skipped'));
            
            // Debug output
            \error_log(sprintf(
                'Import totals - Inserted: %d, Updated: %d, Skipped: %d, DryRun: %s',
                $totalInserted,
                $totalUpdated,
                $totalSkipped,
                $dryRun ? 'yes' : 'no'
            ));
            
            if ($dryRun) {
                $successMessage = '✓ SIMULATION ERFOLGREICH';
                if ($totalInserted === 0 && $totalUpdated === 0 && $totalSkipped === 0) {
                    $successMessage .= ' (keine Daten gefunden)';
                } else {
                    $successMessage .= sprintf(
                        ': %d würden eingefügt, %d würden aktualisiert, %d würden übersprungen.',
                        $totalInserted,
                        $totalUpdated,
                        $totalSkipped
                    );
                }
            } else {
                $successMessage = 'Import abgeschlossen';
                $successMessage .= sprintf(
                    ': %d eingefügt, %d aktualisiert, %d übersprungen.',
                    $totalInserted,
                    $totalUpdated,
                    $totalSkipped
                );
            }
            
            $this->Template->statusMessage = $successMessage;
            $this->Template->stats = $stats;
            $this->Template->formData = $formData;
        } catch (\Throwable $exception) {
            \error_log('ImportError: ' . $exception->getMessage() . "\n" . $exception->getTraceAsString());
            
            $this->Template->statusType = 'error';
            
            $errorDetails = sprintf(
                "<strong>%s</strong><br><br><strong>Datei:</strong> %s (Zeile %d)<br><br><strong>Stack Trace:</strong><br><pre>%s</pre>",
                htmlspecialchars($exception->getMessage(), ENT_QUOTES, 'UTF-8'),
                htmlspecialchars($exception->getFile(), ENT_QUOTES, 'UTF-8'),
                $exception->getLine(),
                htmlspecialchars($exception->getTraceAsString(), ENT_QUOTES, 'UTF-8')
            );
            
            $this->Template->statusMessage = $errorDetails;
            $this->Template->formData = $formData;
        }
    }

    private function inputValue(string $name, string $default): string
    {
        $value = Input::post($name);

        if (null === $value) {
            return $default;
        }

        return trim((string) $value);
    }

    private function buildLegacyDatabaseUrl(string $host, string $port, string $database, string $user, string $password): ?string
    {
        $host = trim($host);
        $port = trim($port);
        $database = trim($database);
        $user = trim($user);

        if ('' === $host || '' === $database || '' === $user || '' === $port || !ctype_digit($port)) {
            return null;
        }

        return sprintf(
            'mysql://%s:%s@%s:%d/%s?charset=utf8mb4',
            rawurlencode($user),
            rawurlencode($password),
            $host,
            (int) $port,
            rawurlencode($database)
        );
    }

    /**
     * @return list<int>|null
     */
    private function parseArchiveIds(string $rawValue): ?array
    {
        $rawValue = trim($rawValue);

        if ('' === $rawValue) {
            return [];
        }

        $parts = array_map('trim', explode(',', $rawValue));
        $ids = [];

        foreach ($parts as $part) {
            if ('' === $part) {
                continue;
            }

            if (!ctype_digit($part)) {
                return null;
            }

            $ids[] = (int) $part;
        }

        return array_values(array_unique($ids));
    }

    private function parseDateValue(string $rawValue, bool $endOfDay): int|false|null
    {
        $rawValue = trim($rawValue);

        if ('' === $rawValue) {
            return null;
        }

        if (ctype_digit($rawValue)) {
            return (int) $rawValue;
        }

        $date = \DateTimeImmutable::createFromFormat('Y-m-d H:i:s', sprintf('%s %s', $rawValue, $endOfDay ? '23:59:59' : '00:00:00'));

        if (false === $date) {
            return false;
        }

        return $date->getTimestamp();
    }
}
