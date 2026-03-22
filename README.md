# ContaoImport

Contao-5-Bundle zum Import von Legacy-News aus einer externen Datenbank.

## Ziele

- Import der News-Archive aus `tl_news_archive`
- Import der News-Metadaten aus `tl_news`
- Import der News-Inhalte aus `tl_content` (`ptable = tl_news`)
- Import der Datei-Referenzen (UUIDs) aus `tl_files` direkt aus der alten Datenbank (empfohlen für konsistente Migration)
- Optional: Import der Datei-Referenzen aus Exportdateien (CSV/JSON)

## Was wird importiert?

- `tl_news_archive` (Archive/Kategorien)
- `tl_news` (News-Metadaten)
- `tl_content` (Inhaltselemente der News, `ptable = tl_news`)
- `tl_files` (Datei-Referenzen inkl. UUIDs, direkt aus alter DB oder per Datei)

## Installation

1. Bundle in ein Contao-5-Projekt einbinden (z. B. als Pfad-Repository oder VCS-Repository).
2. Abhängigkeiten installieren:

   ```bash
   composer require webfarben/contao-news-import
   ```

3. Legacy-DB in der `.env.local` des Contao-Projekts konfigurieren:

   ```dotenv
   LEGACY_DATABASE_URL="pdo-mysql://user:pass@127.0.0.1:3306/legacy_db"
   ```

## Import ausführen

```bash
php vendor/bin/contao-console contao:legacy-news:import
```

## Import im Backend

Nach der Installation erscheint im Contao-Backend unter `System` das Modul `News-Import`.

Dort kannst du den Import ohne Konsole ausführen:

- Zugangsdaten der Quelldatenbank (Host, Port, Datenbank, Benutzer, Passwort)
- optionales Speichern der Zugangsdaten in `localconfig.php`
- Archive-ID-Liste (kommagetrennt, optional)
- Seit/Bis (YYYY-MM-DD oder Timestamp, optional)
- Dry-Run
- News/Inhalte vorab leeren
- optional auch Archive leeren

Nach dem Lauf wird die Tabelle mit Insert/Update/Skip-Werten direkt angezeigt.

Hinweis: Fuer den Import ueber das Backend ist keine `LEGACY_DATABASE_URL` in `.env.local` mehr notwendig, sofern die Quelldatenbank im Formular eingetragen wird.

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

## Wichtiger Hinweis zu Bildern

Alle in den News referenzierten Bilddateien (z. B. für singleSRC, multiSRC, enclosure) müssen vor dem Import manuell und mit korrekter Ordnerstruktur in das Zielverzeichnis `files/` der Contao-Installation kopiert werden. Die Erweiterung übernimmt keine automatische Dateikopie!

Fehlende Dateien führen dazu, dass die Bildreferenzen in den importierten News leer bleiben.
