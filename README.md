# ContaoImport

Contao-5-Bundle zum Import von Legacy-News aus einer externen Datenbank.

## Ziele

- Import der News-Archive aus `tl_news_archive`
- Import der News-Metadaten aus `tl_news`
- Import der News-Inhalte aus `tl_content` (`ptable = tl_news`)
- Wiederholbarer, idempotenter Import per Mapping-Tabelle `tl_contao_import_map`

## Was wird importiert?

- `tl_news_archive` (Archive/Kategorien)
- `tl_news` (News-Metadaten)
- `tl_content` (Inhaltselemente der News, `ptable = tl_news`)

## Installation

1. Bundle in ein Contao-5-Projekt einbinden (z. B. als Pfad-Repository oder VCS-Repository).
2. Abhängigkeiten installieren:

   ```bash
   composer require webfarben/contao-news-import
   ```

3. Legacy-DB in der `.env.local` des Contao-Projekts konfigurieren:

   ```dotenv
   LEGACY_DATABASE_URL="mysql://user:pass@127.0.0.1:3306/legacy_db"
   ```

## Import ausführen

```bash
php vendor/bin/contao-console contao:legacy-news:import
```

Optionen:

- `--dry-run`: Nur simulieren, keine Daten schreiben.
- `--truncate`: Vor dem Import `tl_news` und News-bezogene `tl_content`-Datensätze löschen.
- `--truncate-archives`: Nur zusammen mit `--truncate`; löscht zusätzlich `tl_news_archive`.
- `--archive-id=ID`: Nur bestimmte Archive importieren (mehrfach nutzbar).
- `--since=YYYY-MM-DD|TIMESTAMP`: Nur News ab Datum importieren.
- `--until=YYYY-MM-DD|TIMESTAMP`: Nur News bis Datum importieren.

Beispiel Dry-Run:

```bash
php vendor/bin/contao-console contao:legacy-news:import --dry-run
```

Beispiel nur fuer zwei Archive und Datumsbereich:

```bash
php vendor/bin/contao-console contao:legacy-news:import \
   --archive-id=3 \
   --archive-id=8 \
   --since=2024-01-01 \
   --until=2024-12-31
```

## Mapping konfigurieren

Das Bundle kann Legacy-Spalten auf Zielspalten mappen. Dazu in der konsumierenden App (nicht im Vendor-Ordner) z. B. in `config/services.yaml` konfigurieren:

```yaml
parameters:
   contao_import.column_map:
      tl_news:
         legacy_headline: headline
         legacy_teaser: teaser
      tl_content:
         legacy_text: text
```

Feste Werte pro Tabelle koennen ebenfalls gesetzt werden:

```yaml
parameters:
   contao_import.fixed_values:
      tl_content:
         ptable: tl_news
```

## Idempotenz

Beim ersten echten Lauf erzeugt der Importer die Tabelle `tl_contao_import_map`.

- Speichert Zuordnung `source_table + source_id -> target_id`
- Speichert pro Datensatz einen Hash
- Ueberspringt bei unveraenderten Daten den Schreibvorgang

So bleiben wiederholte Importe schnell und stabil.

## GitHub Packages

Wenn du das Bundle ueber GitHub bereitstellen willst, siehe [docs/GITHUB_PACKAGES.md](docs/GITHUB_PACKAGES.md).
