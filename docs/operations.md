# Drift og oppsett

Sjå [README](../README.md) for lokal oppstart. Dette er driftsrutinar for implementasjonen i repositoriet; miljøet på PythonAnywhere er ikkje kontrollert i denne gjennomgangen.

## Konfigurasjon

`.env` blir lesen frå gjeldande arbeidsmappe, eller frå fila i `FROST_ENV_FILE`. Eksisterande miljøvariablar har prioritet. Bruk absolutte stiar i produksjon, og hald innloggingsopplysningar utanfor dokumentasjon og versjonskontroll.

| Variabel | Standard / funksjon |
| --- | --- |
| `DATABASE_URL` | `sqlite:///frost_local.db`; SQLite for lokal bruk, MySQL kan brukast i produksjon |
| `FROST_CLIENT_ID` | Påkravd for `app.py` og CLI-jobbane for metadata, timejobb og rydding |
| `NVE_HYDAPI_KEY` | Aktiverer NVE |
| `SNOWER_API_KEY` | `snk_`-nøkkel; erstattar eldre brukarnamn/passord |
| `SNOWER_DOMAIN_ID` | Set til t.d. `frost-sync`. CLI krev verdi for å opprette Snower-klienten; webdelen har tilbakefall til `frost-sync` |
| `TILSIG_API_KEY`, `WYSSEN_API_KEY`, `SKRED_API_TOKEN` | Aktiverer respektive kjelder |
| `NVE_VEPS_BASE_URL` | `https://vepsapi.nve.no`; HBV blir aktivert saman med NVE-integrasjonen |
| `NVE_HYDAPI_BASE_URL` | `https://hydapi.nve.no/api/v1` |
| `SNOWER_BASE_URL` | `https://app.snower.fi/api` |
| `TILSIG_BASE_URL` | `https://api.tilsig.com` |
| `SKRED_BASE_URL` | `https://api.skredvarsling.no` |
| `FROST_TIMEOUT_SECONDS` | `60`, brukt av leverandørklientane; retries kan auke samla ventetid |
| `FROST_PAGE_LIMIT` | `1000` |
| `FROST_SOURCE_BATCH_SIZE` | `25` |
| `FROST_RETENTION_DAYS` | `14`, brukt ved rydding av observasjonshistorikk |
| `FROST_SNOW_LOOKBACK_HOURS` | `48`, minst 1; tilbakeblikk for Frost-snø |
| `FROST_QUALITY_CODES` | `0,1,2,3,4` for Frost |
| `FROST_GEOJSON_CACHE_DIR` | `geojson_cache`, relativt til prosjektroten når stien ikkje er absolutt |
| `FROST_ROUTE_PREFIX` | `/weather`, berre i `app.py` |
| `FROST_USER_AGENT` | Identifikasjon for dei eldre Frost-rutene; set ein passande kontaktverdi |
| `CACHE_TYPE`, `CACHE_DEFAULT_TIMEOUT` | `SimpleCache`, `300` for Flask-Caching i `app.py`; dette er separat frå GeoJSON-filene |

Wyssen brukar klienten si standardadresse `https://control.wyssenavalanche.com/app/api/time-series/`. Ein fungerande `WYSSEN_BASE_URL`-overstyring er ikkje implementert. `FROST_BASE_URL` blir heller ikkje lesen frå miljøet; Frost-adressa ligg i `Settings`.

## Jobbar og rekkjefølgje

Databasen lagrar `stations` (metadata, koordinatar, leverandør og `provider_context`), `station_capabilities` (kjende måleeigenskapar), `observations` (målehistorikk), `station_latest` (flate kartverdiar med avleidde aggregat), `nve_discharge_percentiles` og `nve_discharge_flood_thresholds`. `provider_context` inneheld leverandørspesifikke referansar som trengst ved nye datakall; dette er ikkje ein del av den offentlege klientkontrakten.

| Kommando | Når | Resultat |
| --- | --- | --- |
| `python -m frost_sync init-db` | Første oppsett og ved aktuelle skjemaendringar | Opprettar tabellar og dei støtta tilleggskolonnane/backfill; ikkje eit generelt migreringssystem |
| `python -m frost_sync sync-metadata` | Dagleg og etter endringar i kjelder | Oppdaterer stasjonar, provider-context, måleeigenskapar og NVE-statistikk |
| `python -m frost_sync run-hourly` | Kvar time | Hentar observasjonar, oppdaterer latest, committar og byggjer dei fem GeoJSON-filene |
| `python -m frost_sync prune-db` | Dagleg | Slettar eldre historikk og dårlege kvalitetsrader; beheld latest |
| `python -m frost_sync refresh-geojson-cache` | Ved behov | Byggjer latest, FME, compact, 7d og daily, med gzip-variantar; hentar ingen nye målingar |

`sync-metadata` byggjer ikkje GeoJSON-cache. Etter nye kjelder: køyr metadata, så timejobb. Unngå overlappande timejobbar og cachebyggingar; alle brukar dei same `.tmp`-filnamna. Utskrift med `station_errors > 0` eller kjeldefeil i loggen krev oppfølging sjølv om prosessen avsluttar normalt.

Kvar cachefil blir skriven til ei mellombels fil og erstatta. Heile samlinga av JSON/gzip-filer blir ikkje bytt atomisk, og databasecommit skjer før cachebygging. Ein feil her kan gi nye databaseverdiar med eldre kartfiler.

Standardretensjon på 14 dagar gir rom for sjudøgnslag med 24 timars ekstra nedbørstilbakeblikk. For komplett `precipitation_24h_max` i heile sjudøgnsvindauget trengst minst åtte dagar med faktisk innsamla historikk. Retensjon fyller ikkje datagap og startar ikkje historisk tilbakefylling.

## PythonAnywhere / WSGI

1. Legg prosjektet i til dømes `/home/yourusername/frost_codex`, opprett eit virtuelt miljø med Python 3.11+ og installer `requirements.txt`.
2. Konfigurer database og kjeldenøklar i `.env`. MySQL-URL har forma `mysql+pymysql://user:password@host/user$weather?charset=utf8mb4`; URL-kod spesialteikn i legitimasjonen.
3. Endre `PROJECT_HOME` i `pythonanywhere_wsgi.py` til den faktiske prosjektstien. Kople webappen til dette WSGI-oppsettet og same virtuelle miljø.
4. Køyr `init-db`, `sync-metadata`, `run-hourly` med same konfigurasjon som webappen.
5. Set opp jobbar for timejobb, metadata og rydding. Bruk korrekt arbeidsmappe og absolutt sti til miljøet sin Python. Last webappen på nytt etter kode- eller konfigurasjonsendringar.

Til dømes kan timejobben køyrast med dette i Bash:

```bash
cd /home/yourusername/frost_codex && /home/yourusername/.virtualenvs/frostenv/bin/python -m frost_sync run-hourly
```

MySQL-oppsettet i `db.py` brukar `pool_pre_ping=True` og `pool_recycle=280`.

## Kontroll og feilsøking

- **Kartet svarar, men data blir gamle:** kontroller logg for siste timejobb, endringstid på alle fem cachefiler og `*_observed_at`. `/health` sjekkar berre webprosessen. `has_recent_data` i ei cachefil er berekna ved bygging.
- **Ny stasjon manglar:** kontroller aktivering/nøkkel, køyr metadata og timejobb, og sjekk stasjonsdetalj, koordinatar og om stasjonen faktisk har observasjonar.
- **Graf er annleis enn kart:** grafen les direkte frå kjelda, medan kartet les cache/database med eigne filter. Sjekk måletid, eining, aggregat og tilgjengeleg historikk.
- **Tomt eller delvis grafresultat:** les `missing` og `errors` ved fleirstasjonskall. Sjekk om parameteren er støtta av kjelda og om leverandøren avkortar historikken.
- **Historisk daily-lag er tomt:** berre lokal, behalden historikk blir brukt. Datoen er starten på kl. 06–06-perioden i Europe/Oslo.

Ved utrulling av endra berekningsreglar: oppdater skjema der det trengst, køyr aktuelle synkjobbar, bygg cache på nytt og last webappen på nytt. Kontroller eit representativt utval stasjonar frå kvar kjelde og handteringa av gamle/manglande verdiar i Respons.
