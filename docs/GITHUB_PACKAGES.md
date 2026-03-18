# GitHub: Repository, Actions und Composer Installation

Diese Anleitung nutzt dein Repository:

- https://github.com/webfarben/contao_news_import.git

## 1. Repository vorbereiten

- Code in das GitHub-Repository pushen.
- Fuer Releases semantische Tags verwenden, z. B. v1.0.0.

## 2. Composer-Metadaten

In [composer.json](../composer.json) sind die relevanten Felder auf GitHub angepasst:

- name: webfarben/contao-news-import
- homepage: https://github.com/webfarben/contao_news_import
- type: contao-bundle

## 3. GitHub Action aktivieren

Die Datei [.github/workflows/publish-package.yml](../.github/workflows/publish-package.yml) triggert auf Tag-Push (v*) und schickt einen POST an deinen Package-Webhook.

Benutzte Repository-Secrets:

- PACKAGES_WEBHOOK_URL (Pflicht)
- PACKAGES_WEBHOOK_TOKEN (optional)

Gesendete Informationen:

- github.repository
- github.ref_name
- github.sha

## 4. Installation im Contao-Projekt

### Option A: Direkt aus GitHub als VCS

In der Composer-Konfiguration des Contao-Projekts:

```json
{
  "repositories": [
    {
      "type": "vcs",
      "url": "https://github.com/webfarben/contao_news_import.git"
    }
  ]
}
```

Dann installieren:

```bash
composer require webfarben/contao-news-import:dev-main
```

Fuer einen Tag/Release z. B.:

```bash
composer require webfarben/contao-news-import:^1.0
```

### Option B: Packagist anbinden (empfohlen fuer oeffentliche Pakete)

- Repository bei Packagist registrieren.
- In Packagist den GitHub-Service-Hook/Webhook aktivieren.

Dann reicht im Contao-Projekt:

```bash
composer require webfarben/contao-news-import
```

## 5. Release-Ablauf

```bash
git tag v1.0.0
git push origin v1.0.0
```

Damit startet die GitHub Action und benachrichtigt deinen Package-Endpunkt.
