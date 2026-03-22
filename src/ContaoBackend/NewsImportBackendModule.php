<?php

declare(strict_types=1);

namespace webfarben\ContaoImport\ContaoBackend;

use Contao\BackendModule;
use Contao\Config;
use Contao\Controller;
use Contao\Environment;
use Contao\Input;
use Contao\StringUtil;
use Contao\System;
use webfarben\ContaoImport\Import\ImportOptions;
use webfarben\ContaoImport\Import\NewsImporter;

class NewsImportBackendModule extends BackendModule
{
    protected $strTemplate = 'be_contao_news_import';

    protected function compile(): void
    {
        // Hinweis für Nutzer:innen zur Bilddatei-Kopie
        $this->Template->imageCopyNotice = 'Hinweis: Alle in den News referenzierten Bilddateien (z. B. für singleSRC, multiSRC, enclosure) müssen vor dem Import manuell und mit korrekter Ordnerstruktur in das Zielverzeichnis <strong>files/</strong> der Contao-Installation kopiert werden. Die Erweiterung übernimmt keine automatische Dateikopie! Fehlende Dateien führen dazu, dass die Bildreferenzen in den importierten News leer bleiben.';
        $isSubmit = 'tl_contao_news_import' === Input::post('FORM_SUBMIT');
        $storedFormData = $this->loadPersistedFormData();

        $formData = [
            'source_host' => $this->inputValue('source_host', (string) ($storedFormData['source_host'] ?? Config::get('contaoNewsImportSourceHost'))),
            'source_port' => $this->inputValue('source_port', (string) ($storedFormData['source_port'] ?? Config::get('contaoNewsImportSourcePort'))),
            'source_database' => $this->inputValue('source_database', (string) ($storedFormData['source_database'] ?? Config::get('contaoNewsImportSourceDatabase'))),
            'source_user' => $this->inputValue('source_user', (string) ($storedFormData['source_user'] ?? Config::get('contaoNewsImportSourceUser'))),
            'source_password' => $this->inputValue('source_password', (string) ($storedFormData['source_password'] ?? '')),
            'archive_ids' => $this->inputValue('archive_ids', (string) ($storedFormData['archive_ids'] ?? '')),
            'since' => $this->inputValue('since', (string) ($storedFormData['since'] ?? '')),
            'until' => $this->inputValue('until', (string) ($storedFormData['until'] ?? '')),
            'files_dir' => $this->inputValue('files_dir', (string) ($storedFormData['files_dir'] ?? 'files/')),
            'dry_run' => $isSubmit ? '1' === Input::post('dry_run') : (bool) ($storedFormData['dry_run'] ?? false),
            'truncate' => $isSubmit ? '1' === Input::post('truncate') : (bool) ($storedFormData['truncate'] ?? false),
            'truncate_archives' => $isSubmit ? '1' === Input::post('truncate_archives') : (bool) ($storedFormData['truncate_archives'] ?? false),
            'save_credentials' => $isSubmit ? '1' === Input::post('save_credentials') : (bool) ($storedFormData['save_credentials'] ?? false),
            'import_legacy_files_db' => $isSubmit ? '1' === Input::post('import_legacy_files_db') : (bool) ($storedFormData['import_legacy_files_db'] ?? false),
            'symlink_files' => $this->inputValue('symlink_files', (string) ($storedFormData['symlink_files'] ?? '')),
        ];
        // Optional: Symlink public/files anlegen, falls gewünscht
        if ($isSubmit && !empty($formData['symlink_files'])) {
            $publicDir = TL_ROOT . '/public';
            $target = $publicDir . '/files';
            $source = TL_ROOT . '/files';
            if (!is_link($target) && !is_dir($target)) {
                try {
                    if (!is_dir($publicDir)) {
                        throw new \RuntimeException('public/-Verzeichnis nicht gefunden!');
                    }
                    if (!is_dir($source)) {
                        throw new \RuntimeException('Quellverzeichnis files/ nicht gefunden!');
                    }
                    symlink($source, $target);
                    $this->setFlash('success', 'Symlink public/files → files/ wurde erfolgreich angelegt.');
                } catch (\Throwable $e) {
                    $this->setFlash('error', 'Symlink konnte nicht angelegt werden: ' . $e->getMessage());
                    $this->persistFormData($formData, false);
                    $this->persistResultState(null, false);
                    $this->redirectAfterSubmit();
                    return;
                }
            } else {
                $this->setFlash('success', 'Symlink oder Verzeichnis public/files existiert bereits.');
            }
            $this->persistFormData($formData, false);
            $this->persistResultState(null, false);
            $this->redirectAfterSubmit();
            return;
        }
        // Nur noch Import direkt aus der Quelldatenbank möglich
        if ($isSubmit && $formData['import_legacy_files_db']) {
            try {
                $legacyDatabaseUrl = $this->buildLegacyDatabaseUrl(
                    (string) $formData['source_host'],
                    (string) $formData['source_port'],
                    (string) $formData['source_database'],
                    (string) $formData['source_user'],
                    (string) $formData['source_password']
                );
                if (null === $legacyDatabaseUrl) {
                    $this->setFlash('error', 'Bitte Host, Port, Datenbank und Benutzer fuer die Quelldatenbank korrekt eintragen.');
                    $this->persistFormData($formData, false);
                    $this->persistResultState(null, false);
                    $this->redirectAfterSubmit();
                    return;
                }
                $legacyConnectionFactory = System::getContainer()->get('webfarben\ContaoImport\Import\LegacyConnectionFactory');
                $legacyConnection = $legacyConnectionFactory->getConnection($legacyDatabaseUrl);
                /** @var NewsImporter $importer */
                $importer = System::getContainer()->get(NewsImporter::class);
                $importCount = $importer->importLegacyFilesFromDb($legacyConnection, $formData['files_dir'], (bool)$formData['dry_run']);
                // Ergebnis als Statistik-Array für Feedback-Panel
                $stats = [
                    'tl_files' => [
                        'inserted' => $importCount,
                        'updated' => 0,
                        'skipped' => 0,
                    ],
                ];
                $msg = $formData['dry_run']
                    ? sprintf('Simulation: %d tl_files-Einträge würden importiert (keine Änderungen vorgenommen).', $importCount)
                    : sprintf('tl_files-Import aus alter DB abgeschlossen: %d Einträge übernommen.', $importCount);
                $this->setFlash('success', $msg);
                $this->persistFormData($formData, false);
                $this->persistResultState($stats, false);
            } catch (\Throwable $e) {
                $this->setFlash('error', 'Fehler beim Import der tl_files aus der alten DB: ' . $e->getMessage());
                $this->persistFormData($formData, false);
                $this->persistResultState(null, false);
            }
            $this->redirectAfterSubmit();
            return;
        }

        if ($isSubmit && '' === (string) $formData['source_password']) {
            // Erlaubt weiterhin gespeicherte Zugangsdaten, ohne das Passwort beim spaeteren Seitenaufruf vorzubefuellen.
            $formData['source_password'] = (string) Config::get('contaoNewsImportSourcePassword');
        }

        $this->Template->headline = 'Legacy-News-Import';
        $this->Template->imageCopyNotice = $this->Template->imageCopyNotice;
        $this->Template->action = StringUtil::ampersand(Environment::get('request'));
        $this->Template->requestToken = $this->resolveRequestToken();
        $this->Template->statusMessage = '';
        $this->Template->statusType = '';
        $this->Template->stats = null;
        $this->Template->showNoImportInfo = false;
        $this->Template->formData = $formData;
        $this->Template->messages = '';

        if (!$isSubmit) {
            $feedback = $this->consumeFlash();
            $this->Template->statusType = $feedback['type'];
            $this->Template->statusMessage = $feedback['message'];
            $resultState = $this->consumeResultState();
            $this->Template->stats = $resultState['stats'];
            $this->Template->showNoImportInfo = $resultState['showNoImportInfo'];
            return;
        }

        $dryRun = $formData['dry_run'];
        $truncate = $formData['truncate'];
        $truncateArchives = $formData['truncate_archives'];
        $saveCredentials = $formData['save_credentials'];
        $action = (string) Input::post('action');
        $rememberPassword = 'test_connection' === $action || ('run_import' === $action && $dryRun);

        $legacyDatabaseUrl = $this->buildLegacyDatabaseUrl(
            (string) $formData['source_host'],
            (string) $formData['source_port'],
            (string) $formData['source_database'],
            (string) $formData['source_user'],
            (string) $formData['source_password']
        );

        if (null === $legacyDatabaseUrl) {
            $this->setFlash('error', 'Bitte Host, Port, Datenbank und Benutzer fuer die Quelldatenbank korrekt eintragen.');
            $this->persistFormData($formData, $rememberPassword);
            $this->persistResultState(null, false);
            $this->redirectAfterSubmit();
            return;
        }

        if ('test_connection' === $action) {
            try {
                $legacyConnectionFactory = System::getContainer()->get('webfarben\ContaoImport\Import\LegacyConnectionFactory');
                $testConnection = $legacyConnectionFactory->getConnection($legacyDatabaseUrl);
                $testConnection->executeQuery('SELECT 1');

                $this->setFlash('success', 'Verbindung erfolgreich. Die Quelldatenbank ist erreichbar.');
            } catch (\Throwable $e) {
                $this->setFlash('error', 'Verbindung fehlgeschlagen: ' . $e->getMessage());
            }
            $this->persistFormData($formData, true);
            $this->persistResultState(null, false);
            $this->redirectAfterSubmit();
            return;
        }

        try {
            $legacyConnectionFactory = System::getContainer()->get('webfarben\ContaoImport\Import\LegacyConnectionFactory');
            $testConnection = $legacyConnectionFactory->getConnection($legacyDatabaseUrl);
            $testConnection->executeQuery('SELECT 1');
        } catch (\Throwable $e) {
            $this->setFlash('error', 'Verbindung zur Quelldatenbank fehlgeschlagen: ' . $e->getMessage());
            $this->persistFormData($formData, $rememberPassword);
            $this->persistResultState(null, false);
            $this->redirectAfterSubmit();
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
            $this->setFlash('error', 'Die Option "Archive loeschen" funktioniert nur zusammen mit "News/Inhalte leeren".');
            $this->persistFormData($formData, $rememberPassword);
            $this->persistResultState(null, false);
            $this->redirectAfterSubmit();
            return;
        }

        $archiveIds = $this->parseArchiveIds((string) $formData['archive_ids']);

        if (null === $archiveIds) {
            $this->setFlash('error', 'Archive-ID-Liste ist ungueltig. Bitte kommagetrennte Zahlen eintragen.');
            $this->persistFormData($formData, $rememberPassword);
            $this->persistResultState(null, false);
            $this->redirectAfterSubmit();
            return;
        }

        $since = $this->parseDateValue((string) $formData['since'], false);
        $until = $this->parseDateValue((string) $formData['until'], true);

        if (false === $since || false === $until) {
            $this->setFlash('error', 'Datumswerte muessen YYYY-MM-DD oder Unix-Timestamp sein.');
            $this->persistFormData($formData, $rememberPassword);
            $this->persistResultState(null, false);
            $this->redirectAfterSubmit();
            return;
        }

        if (null !== $since && null !== $until && $since > $until) {
            $this->setFlash('error', '"Seit" darf nicht groesser als "Bis" sein.');
            $this->persistFormData($formData, $rememberPassword);
            $this->persistResultState(null, false);
            $this->redirectAfterSubmit();
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
            filesDir: $formData['files_dir'],
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

            $this->setFlash('success', $successMessage);
            $showNoImportInfo = !$dryRun && 0 === $totalInserted && 0 === $totalUpdated && 0 === $totalSkipped;
            $this->persistResultState($stats, $showNoImportInfo);
        } catch (\Throwable $exception) {
            $message = sprintf(
                'Import fehlgeschlagen: %s (Datei: %s, Zeile: %d)',
                $exception->getMessage(),
                $exception->getFile(),
                $exception->getLine()
            );
            $this->setFlash('error', $message);
            $this->persistResultState(null, false);
        }

        $this->persistFormData($formData, $rememberPassword);
        $this->redirectAfterSubmit();
    }

    /**
     * @param array<string, mixed> $formData
     */
    private function persistFormData(array $formData, bool $rememberPassword = false): void
    {
        Config::persist('contaoNewsImportLastSourceHost', (string) ($formData['source_host'] ?? ''));
        Config::persist('contaoNewsImportLastSourcePort', (string) ($formData['source_port'] ?? ''));
        Config::persist('contaoNewsImportLastSourceDatabase', (string) ($formData['source_database'] ?? ''));
        Config::persist('contaoNewsImportLastSourceUser', (string) ($formData['source_user'] ?? ''));
        Config::persist('contaoNewsImportLastSourcePassword', '');
        Config::persist('contaoNewsImportLastArchiveIds', (string) ($formData['archive_ids'] ?? ''));
        Config::persist('contaoNewsImportLastSince', (string) ($formData['since'] ?? ''));
        Config::persist('contaoNewsImportLastUntil', (string) ($formData['until'] ?? ''));
        Config::persist('contaoNewsImportLastDryRun', !empty($formData['dry_run']) ? '1' : '');
        Config::persist('contaoNewsImportLastTruncate', !empty($formData['truncate']) ? '1' : '');
        Config::persist('contaoNewsImportLastTruncateArchives', !empty($formData['truncate_archives']) ? '1' : '');
        Config::persist('contaoNewsImportLastSaveCredentials', !empty($formData['save_credentials']) ? '1' : '');

        if ($rememberPassword && !empty($formData['source_password'])) {
            $_SESSION['contao_news_import_temp_source_password'] = (string) $formData['source_password'];
            return;
        }

        unset($_SESSION['contao_news_import_temp_source_password']);
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
            'source_password' => $this->consumeTemporarySourcePassword(),
            'archive_ids' => (string) Config::get('contaoNewsImportLastArchiveIds'),
            'since' => (string) Config::get('contaoNewsImportLastSince'),
            'until' => (string) Config::get('contaoNewsImportLastUntil'),
            'dry_run' => '1' === (string) Config::get('contaoNewsImportLastDryRun'),
            'truncate' => '1' === (string) Config::get('contaoNewsImportLastTruncate'),
            'truncate_archives' => '1' === (string) Config::get('contaoNewsImportLastTruncateArchives'),
            'save_credentials' => '1' === (string) Config::get('contaoNewsImportLastSaveCredentials'),
            'import_legacy_files_db' => false,
        ];
    }

    private function consumeTemporarySourcePassword(): string
    {
        $password = (string) ($_SESSION['contao_news_import_temp_source_password'] ?? '');
        unset($_SESSION['contao_news_import_temp_source_password']);

        return $password;
    }

    private function setFlash(string $type, string $message): void
    {
        $_SESSION['contao_import_flash'] = ['type' => $type, 'message' => $message];
    }

    /**
     * @return array{type: string, message: string}
     */
    private function consumeFlash(): array
    {
        $flash = $_SESSION['contao_import_flash'] ?? null;
        unset($_SESSION['contao_import_flash']);

        if (!\is_array($flash)) {
            return ['type' => '', 'message' => ''];
        }

        return [
            'type'    => (string) ($flash['type'] ?? ''),
            'message' => (string) ($flash['message'] ?? ''),
        ];
    }

    /**
     * @param array<string, array<string, int>>|null $stats
     */
    private function persistResultState(?array $stats, bool $showNoImportInfo): void
    {
        $_SESSION['contao_news_import_result_state'] = [
            'stats'           => $stats,
            'showNoImportInfo' => $showNoImportInfo,
        ];
    }

    /**
     * @return array{stats: array<string, array<string, int>>|null, showNoImportInfo: bool}
     */
    private function consumeResultState(): array
    {
        $result = $_SESSION['contao_news_import_result_state'] ?? null;
        unset($_SESSION['contao_news_import_result_state']);

        if (!\is_array($result)) {
            return ['stats' => null, 'showNoImportInfo' => false];
        }

        return [
            'stats'           => isset($result['stats']) && \is_array($result['stats']) ? $result['stats'] : null,
            'showNoImportInfo' => !empty($result['showNoImportInfo']),
        ];
    }

    private function redirectAfterSubmit(): void
    {
        Controller::redirect(Environment::get('requestUri'));
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
