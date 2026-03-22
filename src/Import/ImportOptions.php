<?php

declare(strict_types=1);

namespace webfarben\ContaoImport\Import;

class ImportOptions
{
    /**
     * @param list<int> $archiveIds
     */
    public function __construct(
        public readonly bool $dryRun,
        public readonly bool $truncate,
        public readonly bool $truncateArchives,
        public readonly array $archiveIds = [],
        public readonly ?int $since = null,
        public readonly ?int $until = null,
        public readonly ?string $legacyDatabaseUrl = null,
    ) {
    }
}
