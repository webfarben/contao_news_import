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
        $storedFormData = $this->loadPersistedFormData();

        $formData = [
            'source_host' => $this->inputValue('source_host', (string) ($storedFormData['source_host'] ?? Config::get('contaoNewsImportSourceHost'))),
            'source_port' => $this->inputValue('source_port', (string) ($storedFormData['source_port'] ?? Config::get('contaoNewsImportSourcePort'))),
            'source_database' => $this->inputValue('source_database', (string) ($storedFormData['source_database'] ?? Config::get('contaoNewsImportSourceDatabase'))),
            'source_user' => $this->inputValue('source_user', (string) ($storedFormData['source_user'] ?? Config::get('contaoNewsImportSourceUser'))),
            'source_password' => $this->inputValue('source_password', (string) ($storedFormData['source_password'] ?? Config::get('contaoNewsImportSourcePassword'))),
            'archive_ids' => $this->inputValue('archive_ids', (string) ($storedFormData['archive_ids'] ?? '')),
            'since' => $this->inputValue('since', (string) ($storedFormData['since'] ?? '')),
            'until' => $this->inputValue('until', (string) ($storedFormData['until'] ?? '')),
            'dry_run' => '1' === Input::post('dry_run'),
            'truncate' => '1' === Input::post('truncate'),
            'truncate_archives' => '1' === Input::post('truncate_archives'),
            'save_credentials' => '1' === Input::post('save_credentials'),
        ];

        if ('tl_contao_news_import' !== Input::post('FORM_SUBMIT') && [] !== $storedFormData) {
            $formData['dry_run'] = (bool) ($storedFormData['dry_run'] ?? false);
            $formData['truncate'] = (bool) ($storedFormData['truncate'] ?? false);
            $formData['truncate_archives'] = (bool) ($storedFormData['truncate_archives'] ?? false);
            $formData['save_credentials'] = (bool) ($storedFormData['save_credentials'] ?? false);
        }

        $this->Template->headline = 'Legacy-News-Import';
        $this->Template->action = StringUtil::ampersand(Environment::get('request'));
        $this->Template->requestToken = $this->resolveRequestToken();
        $this->Template->statusMessage = (string) Config::get('contaoNewsImportLastStatusMessage');
        $this->Template->statusType = (string) Config::get('contaoNewsImportLastStatusType');
        $this->Template->stats = null;
        $this->Template->formData = $formData;
        $this->Template->messages = '';

        if ('tl_contao_news_import' !== Input::post('FORM_SUBMIT')) {
            return;
        }

        $dryRun = $formData['dry_run'];
        $truncate = $formData['truncate'];
        $truncateArchives = $formData['truncate_archives'];
        $saveCredentials = $formData['save_credentials'];
        $action = (string) Input::post('action');

        $legacyDatabaseUrl = $this->buildLegacyDatabaseUrl(
            (string) $formData['source_host'],
            (string) $formData['source_port'],
            (string) $formData['source_database'],
            (string) $formData['source_user'],
            (string) $formData['source_password']
        );

        if (null === $legacyDatabaseUrl) {
            $this->setFeedback('error', 'Bitte Host, Port, Datenbank und Benutzer fuer die Quelldatenbank korrekt eintragen.');
            $this->persistFormData($formData);
            $this->Template->formData = $formData;

            return;
        }

        if ('test_connection' === $action) {
            try {
                $legacyConnectionFactory = System::getContainer()->get('Sebastian\ContaoImport\Import\LegacyConnectionFactory');
                $testConnection = $legacyConnectionFactory->getConnection($legacyDatabaseUrl);
                $testConnection->executeQuery('SELECT 1');

                $this->setFeedback('success', 'Verbindung erfolgreich. Die Quelldatenbank ist erreichbar.');
            } catch (\Throwable $e) {
                $this->setFeedback('error', 'Verbindung fehlgeschlagen: ' . $e->getMessage());
            }
            $this->persistFormData($formData);
            $this->Template->formData = $formData;
            return;
        }

        try {
            $legacyConnectionFactory = System::getContainer()->get('Sebastian\ContaoImport\Import\LegacyConnectionFactory');
            $testConnection = $legacyConnectionFactory->getConnection($legacyDatabaseUrl);
            $testConnection->executeQuery('SELECT 1');
        } catch (\Throwable $e) {
            $this->setFeedback('error', 'Verbindung zur Quelldatenbank fehlgeschlagen: ' . $e->getMessage());
            $this->persistFormData($formData);
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
            $this->setFeedback('error', 'Die Option "Archive loeschen" funktioniert nur zusammen mit "News/Inhalte leeren".');
            $this->persistFormData($formData);
            $this->Template->formData = $formData;

            return;
        }

        $archiveIds = $this->parseArchiveIds((string) $formData['archive_ids']);

        if (null === $archiveIds) {
            $this->setFeedback('error', 'Archive-ID-Liste ist ungueltig. Bitte kommagetrennte Zahlen eintragen.');
            $this->persistFormData($formData);
            $this->Template->formData = $formData;

            return;
        }

        $since = $this->parseDateValue((string) $formData['since'], false);
        $until = $this->parseDateValue((string) $formData['until'], true);

        if (false === $since || false === $until) {
            $this->setFeedback('error', 'Datumswerte muessen YYYY-MM-DD oder Unix-Timestamp sein.');
            $this->persistFormData($formData);
            $this->Template->formData = $formData;

            return;
        }

        if (null !== $since && null !== $until && $since > $until) {
            $this->setFeedback('error', '"Seit" darf nicht groesser als "Bis" sein.');
            $this->persistFormData($formData);
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

            $totalInserted = (int) array_sum(array_column($stats, 'inserted'));
            $totalUpdated = (int) array_sum(array_column($stats, 'updated'));
            $totalSkipped = (int) array_sum(array_column($stats, 'skipped'));

            if ($dryRun) {
                $successMessage = 'Simulation erfolgreich';
                if ($totalInserted === 0 && $totalUpdated === 0 && $totalSkipped === 0) {
                    $successMessage .= ' (keine Daten gefunden)';
                } else {
                    $successMessage .= sprintf(
                        ': %d wuerden eingefuegt, %d wuerden aktualisiert, %d wuerden uebersprungen.',
                        $totalInserted,
                        $totalUpdated,
                        $totalSkipped
                    );
                }
            } else {
                $successMessage = 'Import abgeschlossen';
                $successMessage .= sprintf(
                    ': %d eingefuegt, %d aktualisiert, %d uebersprungen.',
                    $totalInserted,
                    $totalUpdated,
                    $totalSkipped
                );
            }

            $this->setFeedback('success', $successMessage);
            $this->persistFormData($formData);
            $this->Template->statusMessage = $successMessage;
            $this->Template->statusType = 'success';
            $this->Template->stats = $stats;
            $this->Template->formData = $formData;
        } catch (\Throwable $exception) {
            $message = sprintf(
                'Import fehlgeschlagen: %s (Datei: %s, Zeile: %d)',
                $exception->getMessage(),
                $exception->getFile(),
                $exception->getLine()
            );
            $this->setFeedback('error', $message);
            $this->persistFormData($formData);
            $this->Template->statusType = 'error';
            $this->Template->statusMessage = $message;
            $this->Template->formData = $formData;
        }
    }

    /**
     * @param array<string, mixed> $formData
     */
    private function persistFormData(array $formData): void
    {
        Config::persist('contaoNewsImportLastSourceHost', (string) ($formData['source_host'] ?? ''));
        Config::persist('contaoNewsImportLastSourcePort', (string) ($formData['source_port'] ?? ''));
        Config::persist('contaoNewsImportLastSourceDatabase', (string) ($formData['source_database'] ?? ''));
        Config::persist('contaoNewsImportLastSourceUser', (string) ($formData['source_user'] ?? ''));
        Config::persist('contaoNewsImportLastSourcePassword', (string) ($formData['source_password'] ?? ''));
        Config::persist('contaoNewsImportLastArchiveIds', (string) ($formData['archive_ids'] ?? ''));
        Config::persist('contaoNewsImportLastSince', (string) ($formData['since'] ?? ''));
        Config::persist('contaoNewsImportLastUntil', (string) ($formData['until'] ?? ''));
        Config::persist('contaoNewsImportLastDryRun', !empty($formData['dry_run']) ? '1' : '');
        Config::persist('contaoNewsImportLastTruncate', !empty($formData['truncate']) ? '1' : '');
        Config::persist('contaoNewsImportLastTruncateArchives', !empty($formData['truncate_archives']) ? '1' : '');
        Config::persist('contaoNewsImportLastSaveCredentials', !empty($formData['save_credentials']) ? '1' : '');
    }

    /**
     * @return array<string, mixed>
     */
    private function loadPersistedFormData(): array
    {
        return [
            'source_host' => (string) Config::get('contaoNewsImportLastSourceHost'),
            'source_port' => (string) Config::get('contaoNewsImportLastSourcePort'),
            'source_database' => (string) Config::get('contaoNewsImportLastSourceDatabase'),
            'source_user' => (string) Config::get('contaoNewsImportLastSourceUser'),
            'source_password' => (string) Config::get('contaoNewsImportLastSourcePassword'),
            'archive_ids' => (string) Config::get('contaoNewsImportLastArchiveIds'),
            'since' => (string) Config::get('contaoNewsImportLastSince'),
            'until' => (string) Config::get('contaoNewsImportLastUntil'),
            'dry_run' => '1' === (string) Config::get('contaoNewsImportLastDryRun'),
            'truncate' => '1' === (string) Config::get('contaoNewsImportLastTruncate'),
            'truncate_archives' => '1' === (string) Config::get('contaoNewsImportLastTruncateArchives'),
            'save_credentials' => '1' === (string) Config::get('contaoNewsImportLastSaveCredentials'),
        ];
    }

    private function setFeedback(string $type, string $message): void
    {
        Config::persist('contaoNewsImportLastStatusType', $type);
        Config::persist('contaoNewsImportLastStatusMessage', $message);

        $this->Template->statusType = $type;
        $this->Template->statusMessage = $message;
        $this->Template->messages = '';
    }

    private function inputValue(string $name, string $default): string
    {
        $value = Input::post($name);

        if (null === $value) {
            return $default;
        }

        return trim((string) $value);
    }

    private function resolveRequestToken(): string
    {
        if (defined('REQUEST_TOKEN') && '' !== (string) REQUEST_TOKEN) {
            return (string) REQUEST_TOKEN;
        }

        $container = System::getContainer();

        if ($container->has('contao.csrf.token_manager')) {
            $tokenManager = $container->get('contao.csrf.token_manager');

            if (\is_object($tokenManager) && \method_exists($tokenManager, 'getDefaultTokenValue')) {
                return (string) $tokenManager->getDefaultTokenValue();
            }
        }

        return '';
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
            'pdo-mysql://%s:%s@%s:%d/%s?charset=utf8mb4',
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
