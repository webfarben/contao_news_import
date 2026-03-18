<?php

declare(strict_types=1);

namespace Sebastian\ContaoImport\Command;

use Sebastian\ContaoImport\Import\ImportOptions;
use Sebastian\ContaoImport\Import\NewsImporter;
use Symfony\Component\Console\Attribute\AsCommand;
use Symfony\Component\Console\Command\Command;
use Symfony\Component\Console\Input\InputInterface;
use Symfony\Component\Console\Input\InputOption;
use Symfony\Component\Console\Output\OutputInterface;
use Symfony\Component\Console\Style\SymfonyStyle;

#[AsCommand(
    name: 'contao:legacy-news:import',
    description: 'Importiert News aus einer Legacy-Datenbank in Contao 5.',
)]
class ImportLegacyNewsCommand extends Command
{
    public function __construct(private readonly NewsImporter $newsImporter)
    {
        parent::__construct();
    }

    protected function configure(): void
    {
        $this
            ->addOption('dry-run', null, InputOption::VALUE_NONE, 'Nur simulieren, nichts schreiben.')
            ->addOption('truncate', null, InputOption::VALUE_NONE, 'Vorher tl_news und tl_content (News-Inhalte) leeren.')
            ->addOption('truncate-archives', null, InputOption::VALUE_NONE, 'Mit --truncate auch tl_news_archive leeren.')
            ->addOption('archive-id', null, InputOption::VALUE_REQUIRED | InputOption::VALUE_IS_ARRAY, 'Nur bestimmte Archive importieren (mehrfach nutzbar).')
            ->addOption('since', null, InputOption::VALUE_REQUIRED, 'Nur News ab Datum (YYYY-MM-DD) oder Unix-Timestamp.')
            ->addOption('until', null, InputOption::VALUE_REQUIRED, 'Nur News bis Datum (YYYY-MM-DD) oder Unix-Timestamp.');
    }

    protected function execute(InputInterface $input, OutputInterface $output): int
    {
        $io = new SymfonyStyle($input, $output);
        $dryRun = (bool) $input->getOption('dry-run');
        $truncate = (bool) $input->getOption('truncate');
        $truncateArchives = (bool) $input->getOption('truncate-archives');
        $archiveIds = $this->parseArchiveIds((array) $input->getOption('archive-id'));
        $since = $this->parseDateOption($input->getOption('since'), false);
        $until = $this->parseDateOption($input->getOption('until'), true);

        if ($truncateArchives && !$truncate) {
            $io->error('Die Option --truncate-archives kann nur zusammen mit --truncate verwendet werden.');

            return Command::INVALID;
        }

        if (null === $archiveIds) {
            $io->error('Mindestens ein Wert fuer --archive-id ist ungueltig.');

            return Command::INVALID;
        }

        if (false === $since || false === $until) {
            $io->error('Die Optionen --since/--until muessen YYYY-MM-DD oder Unix-Timestamp sein.');

            return Command::INVALID;
        }

        if (null !== $since && null !== $until && $since > $until) {
            $io->error('--since darf nicht groesser als --until sein.');

            return Command::INVALID;
        }

        $options = new ImportOptions(
            dryRun: $dryRun,
            truncate: $truncate,
            truncateArchives: $truncateArchives,
            archiveIds: $archiveIds,
            since: $since,
            until: $until,
        );

        $io->title('Legacy-News-Import');
        $io->text(sprintf('Modus: %s', $dryRun ? 'Dry-Run (Simulation)' : 'Schreibend'));

        $stats = $this->newsImporter->import($options);

        $rows = [];
        foreach ($stats as $table => $tableStats) {
            $rows[] = [
                $table,
                (string) $tableStats['inserted'],
                (string) $tableStats['updated'],
                (string) $tableStats['skipped'],
            ];
        }

        $io->table(['Tabelle', 'Inserted', 'Updated', 'Skipped'], $rows);
        $io->success($dryRun ? 'Simulation abgeschlossen.' : 'Import abgeschlossen.');

        return Command::SUCCESS;
    }

    /**
     * @param list<string> $rawArchiveIds
     *
     * @return list<int>|null
     */
    private function parseArchiveIds(array $rawArchiveIds): ?array
    {
        if ([] === $rawArchiveIds) {
            return [];
        }

        $archiveIds = [];

        foreach ($rawArchiveIds as $rawArchiveId) {
            if (!ctype_digit($rawArchiveId)) {
                return null;
            }

            $archiveIds[] = (int) $rawArchiveId;
        }

        return array_values(array_unique($archiveIds));
    }

    private function parseDateOption(mixed $value, bool $endOfDay): int|false|null
    {
        if (null === $value) {
            return null;
        }

        $stringValue = trim((string) $value);
        if ('' === $stringValue) {
            return null;
        }

        if (ctype_digit($stringValue)) {
            return (int) $stringValue;
        }

        $date = \DateTimeImmutable::createFromFormat('Y-m-d H:i:s', sprintf('%s %s', $stringValue, $endOfDay ? '23:59:59' : '00:00:00'));

        if (false === $date) {
            return false;
        }

        return $date->getTimestamp();
    }
}
