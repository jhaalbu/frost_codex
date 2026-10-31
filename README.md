# API-proxy for Respons

Denne tenesta samlar vêr-, snø- og hydrologidata til kartportalen **Respons**, som blir brukt til skredberedskap. Ho normaliserer data frå Frost, NVE HydAPI, Snower, Tilsig, Wyssen og Skred AS til felles stasjonar og parameterar.

Tenesta har to dataflytar:

- **Kartlag:** planlagd synk → database → ferdigbygde GeoJSON-filer → ArcGIS, VertiGIS og FME.
- **Grafar:** klientførespurnad → API hos leverandøren → normalisert tidsserierespons. Stasjonsmetadata må allereie finnast i databasen.

Kart og graf kan vise ulike verdiar og ulik datadekning. Eit nytt kartkall garanterer ikkje nye observasjonar. Bruk tidspunktet for den aktuelle måleverdien når ferskheit skal vurderast.

## Dokumentasjon

| Dokument | Innhald |
| --- | --- |
| [API-referanse](docs/api.md) | Endepunkt, prefiks, parameterar, responsformat og feil |
| [Datakjelder](docs/providers.md) | Autentisering, stasjons-ID-ar og kva integrasjonane leverer |
| [Drift og oppsett](docs/operations.md) | Oppstart, miljøvariablar, PythonAnywhere og feilsøking |
| [Proxy-nøkkel](docs/proxy_auth.md) | Påkravd `X-API-Key`, klientoppsett og nøkkelbyte |
| [Datametodar](docs/data_methods.md) | Kvalitetsfilter, nedbør, snø, døgn- og sjudøgnsberekningar |
| [NVE-klassifisering](docs/nve_discharge_classification.md) | Vassføring, persentilar og flaumklassar |
| [HBV-vassføring](docs/nve_hbv.md) | Modellserie til graf og prognose for neste døgn i latest-kartlaga |
| [Kodegjennomgang 2026-10-28](docs/review-2026-10-28.md) | Konkrete funn og prioriterte oppfølgingspunkt |

## Lokal oppstart

Python 3.11 eller nyare. PowerShell:

```powershell
python -m venv .venv
.\.venv\Scripts\Activate.ps1
python -m pip install -r requirements.txt
```

Opprett `.env` i prosjektroten med nøklar for kjeldene du har tilgang til:

```dotenv
FROST_CLIENT_ID=your-client-id
PROXY_API_KEY=your-own-random-proxy-key
DATABASE_URL=sqlite:///frost_local.db
NVE_HYDAPI_KEY=your-nve-key
SNOWER_API_KEY=snk_your-key
SNOWER_DOMAIN_ID=frost-sync
TILSIG_API_KEY=your-tilsig-key
WYSSEN_API_KEY=your-wyssen-key
SKRED_API_TOKEN=your-skred-token
```

```powershell
python -m frost_sync init-db
python -m frost_sync sync-metadata
python -m frost_sync run-hourly
flask --app app run --host 127.0.0.1 --port 5000
```

Med denne oppstarten ligg kartlaget på `/weather/api/stations/latest.geojson` og parameterkatalogen på `/weather/api/parameters`. `python -m frost_sync serve` eksponerer blueprint-rutene utan `/weather`.

Dei fire `latest*.geojson`-rutene er opne utan nøkkel. Tidsseriar og andre datakall, inkludert daily/historikk, krev `X-API-Key` med verdien frå `PROXY_API_KEY`. Webappen nektar å starte utan ein konfigurert proxy-nøkkel. Sjå [tilgangsoppsettet](docs/proxy_auth.md) før utrulling.

`sync-metadata`, `run-hourly` og `prune-db` krev i dagens CLI at `FROST_CLIENT_ID` er sett, også når andre kjelder er konfigurerte.

## Utvikling

```powershell
.\.venv\Scripts\python.exe -m unittest discover -s tests -v
```

Leverandørklientane ligg i `frost_sync/*_api.py` og `nve_hydapi.py`, synk og avleidde verdiar i `service.py`, database i `models.py`/`db.py`, og responsbygging i `web.py`. `app.py` kombinerer kart-API-et med dei eldre Frost-proxyrutene. Testane brukar lokale data og mockar; dei stadfestar ikkje tilgang til leverandørane i produksjon.
