<?php

declare(strict_types=1);

namespace Sebastian\ContaoImport\ContaoBackend;

use Contao\BackendModule;
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
        $this->Template->headline = 'Legacy-News-Import';
        $this->Template->action = StringUtil::ampersand(Environment::get('request'));
        $this->Template->requestToken = defined('REQUEST_TOKEN') ? REQUEST_TOKEN : '';
        $this->Template->statusMessage = null;
        $this->Template->statusType = null;
        $this->Template->stats = null;

        if ('tl_contao_news_import' !== Input::post('FORM_SUBMIT')) {
            return;
        }

        $dryRun = '1' === Input::post('dry_run');
        $truncate = '1' === Input::post('truncate');
        $truncateArchives = '1' === Input::post('truncate_archives');

        if ($truncateArchives && !$truncate) {
            $this->Template->statusType = 'error';
            $this->Template->statusMessage = 'Die Option "Archive loeschen" funktioniert nur zusammen mit "News/Inhalte leeren".';

            return;
        }

        $archiveIds = $this->parseArchiveIds((string) Input::post('archive_ids'));

        if (null === $archiveIds) {
            $this->Template->statusType = 'error';
            $this->Template->statusMessage = 'Archive-ID-Liste ist ungueltig. Bitte kommagetrennte Zahlen eintragen.';

            return;
        }

        $since = $this->parseDateValue((string) Input::post('since'), false);
        $until = $this->parseDateValue((string) Input::post('until'), true);

        if (false === $since || false === $until) {
            $this->Template->statusType = 'error';
            $this->Template->statusMessage = 'Datumswerte muessen YYYY-MM-DD oder Unix-Timestamp sein.';

            return;
        }

        if (null !== $since && null !== $until && $since > $until) {
            $this->Template->statusType = 'error';
            $this->Template->statusMessage = '"Seit" darf nicht groesser als "Bis" sein.';

            return;
        }

        $options = new ImportOptions(
            dryRun: $dryRun,
            truncate: $truncate,
            truncateArchives: $truncateArchives,
            archiveIds: $archiveIds,
            since: $since,
            until: $until,
        );

        try {
            /** @var NewsImporter $importer */
            $importer = System::getContainer()->get(NewsImporter::class);
            $stats = $importer->import($options);

            $this->Template->statusType = 'success';
            $this->Template->statusMessage = $dryRun ? 'Simulation abgeschlossen.' : 'Import abgeschlossen.';
            $this->Template->stats = $stats;
        } catch (\Throwable $exception) {
            $this->Template->statusType = 'error';
            $this->Template->statusMessage = $exception->getMessage();
        }
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
