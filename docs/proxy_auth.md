# Tilgang med proxy-nøkkel

Latest-GeoJSON er ope utan nøkkel. Tidsseriar og dei andre datarutene krev ein eigen nøkkel i HTTP-headeren `X-API-Key`.

| Tilgang | Ruter (med `/weather` framfor i standardoppsettet) |
| --- | --- |
| Ope | `/api/stations/latest.geojson`, `/api/stations/latest.compact.geojson`, `/api/stations/latest.fme.geojson`, `/api/stations/latest.7d.geojson` |
| Krev nøkkel | `/api/timeseries`, `/api/stations/<source_id>/timeseries`, `daily.geojson`, `history.geojson`, stasjonsdetaljar, observasjonshistorikk, tersklar og parameterkatalog |
| Krev nøkkel, utan prefiks | `/frost`, `/frost_available`, `/sources` |

Unntaket gjeld òg `HEAD`, filtrert latest (`?has=...`), gzip-filer og tilbakefall til database. Det gjeld dei namngjevne rutene, ikkje vilkårlege URL-ar som sluttar på `.geojson`. Daily og history er framleis verna fordi dei gir historikk. Merk at det opne sjudøgnslaget viser historiske aggregat, og at latest-laga òg kan innehalde HBV-prognosen for morgondagen. Alle kan lese data frå dei publiserte kjeldene i desse laga.

Leverandørnøklar frå Snower, Wyssen, NVE og andre kjelder er framleis berre til utgåande kall. **Proxy-nøkkelen skal vere ein eigen verdi**, ikkje ein av leverandørnøklane.

## Oppsett på PythonAnywhere

1. Generer ein tilfeldig nøkkel på ein trygg maskin eller i PythonAnywhere-konsollen:

   ```bash
   python -c "import secrets; print(secrets.token_urlsafe(32))"
   ```

2. Legg nøkkelen i `.env` som webappen les, eller i miljøoppsettet:

   ```dotenv
   PROXY_API_KEY=den-genererte-nokkelen
   ```

3. Konfigurer Respons/FME til å sende `X-API-Key` med same verdi. Last webappen på nytt etter at både kode og miljøvariabel er på plass.

Webappen nektar å starte dersom `PROXY_API_KEY` manglar eller er tom. Det finst ingen automatisk tilbakefallsmodus med ope API. Synkjobbar og lokale eksportbyggjarar treng ikkje proxy-nøkkelen, sidan dei ikkje er innkomande HTTP-kall.

Ved nøkkelbyte: oppdater serveroppsett og klientar, og last webappen på nytt. Den gamle nøkkelen blir då avvist. Det er éin delt nøkkel, utan eigne brukarar, roller eller individuell tilbakekalling.

## Kall frå ein serverklient

Dette Python-dømet les nøkkelen frå klientmiljøet:

```python
import os
import requests

response = requests.get(
    "https://yourusername.pythonanywhere.com/weather/api/stations/12.215.0/timeseries",
    headers={"X-API-Key": os.environ["PROXY_API_KEY"]},
    params={"parameters": "discharge,discharge_hbv", "date": "2026-10-30"},
    timeout=60,
)
response.raise_for_status()
data = response.json()
```

Bruk HTTPS i produksjon. Nøkkelen blir berre godteken i `X-API-Key`, ikkje i querystrengen eller som ein leverandørnøkkel. I FME må headeren setjast på HTTP-førespurnaden.

## Respons og unntak

- Rett nøkkel gir tilgang til dei verna rutene; vanleg validering og feilhandtering gjeld framleis. Latest-GeoJSON krev ingen nøkkel.
- Manglande eller feil nøkkel på ei verna rute gir HTTP `401` med `{"error":"Unauthorized"}`. Svaret avslører ikkje kva nøkkel som var venta.
- `/health` og blueprint-ruta `/weather/health` er opne og returnerer berre `{"status":"ok"}`. Dei sjekkar ikkje nøkkelen eller tilgangen til leverandørane.
- `OPTIONS` er ope for CORS-førespurnader før nettlesaren sender sjølve datakallet. Det gir ingen måledata; etterfølgjande kall til ei verna rute krev nøkkel.
- På verna ruter køyrer nøkkelkontrollen før både filcache, Flask-Caching og leverandørkall. Verna dataresponsar og avvisingar får `Cache-Control: private, no-store` og `Vary: X-API-Key`. Lokal servercache blir framleis brukt etter godkjend kontroll. Opne latest-filer beheld den vanlege HTTP-cachetida på høgst 300 sekund, avgrensa til neste norske midnatt.

Tidlegare offentleg mellomlagra svar kan ikkje trekkjast tilbake med denne kodeendringa. Fjern eventuelle gamle delte HTTP-/CDN-cacheoppføringar ved utrulling. PythonAnywhere sine statiske filtilordningar må heller ikkje publisere `.env`, database eller `geojson_cache` direkte: slike tilordningar blir ikkje handterte av Flask og går utanom headerkontrollen.

## Respons i nettlesaren

Innlogging i Respons vernar ikkje automatisk PythonAnywhere-rutene. Respons må sende nøkkelen i kvart kall til verna ruter, eller la ein backend med tilgangskontroll gjere kalla. Latest-GeoJSON kan hentast direkte utan nøkkel.

Dersom ein fast nøkkel blir send frå JavaScript i nettlesaren, kan innlogga brukarar hente han ut og bruke han andre stader. Headerløysinga vernar då mot klientar som ikkje kjenner nøkkelen, men gir ikkje ei binding til Respons-innlogginga. For at nøkkelen skal vere hemmeleg for sluttbrukarane, må han liggje på backend. CORS er ikkje ein erstatning for nøkkelkontrollen.
