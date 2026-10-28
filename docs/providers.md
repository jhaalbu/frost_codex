# Datakjelder

Dette dokumentet skildrar integrasjonane slik dei er kopla i koden per 2026-10-28. Tilgang og datadekning hos leverandøren er ikkje verifisert med produksjonskall i denne gjennomgangen.

| Kjelde / `provider` | `source_id` | Aktivering | Data i vanleg synk |
| --- | --- | --- | --- |
| MET Frost / `frost` | T.d. `SN18700` | `FROST_CLIENT_ID` | Temperatur, timesnedbør, snødjupn, vind og vindkast der stasjonen har desse |
| NVE / `nve_hydapi` | T.d. `84.21.0` | `NVE_HYDAPI_KEY` | Utvalde seriar, mellom anna vassføring, grunnvatn, snø og nedbør |
| Snower / `snower` | `SNWR-<permanent_id>` | `SNOWER_API_KEY` og, for CLI-synk, `SNOWER_DOMAIN_ID` | `snow_depth` og `probe_temperature` mappa til `air_temperature` |
| Tilsig / `tilsig` | `TILSIG-<station_id>` | `TILSIG_API_KEY` | Berre godkjende Snow Depth Mode-seriar → `snow_depth` |
| Wyssen / `wyssen` | `WYSSEN-<station_id>` | `WYSSEN_API_KEY` | `MTA` → temperatur, `MWS` → vindfart, `MWD` → vindretning |
| Skred AS / `skred` | `SKRED-cs001` | `SKRED_API_TOKEN` | Temperatur, vindfart og vindretning frå Vikafjell |

Koordinatar er nødvendige for å kome med i kartlaget. Ein registrert stasjon utan koordinatar kan framleis finnast via stasjonsdetaljar og tidsseriar. `has_*` skildrar kjende måleeigenskapar, ikkje ein garanti for ferske verdiar.

## Frost og NVE

Frost brukar HTTP Basic med klient-ID som brukarnamn og tomt passord. Metadata kjem frå `sources` og tilgjengelege tidsseriar; observasjonar kjem frå `observations`. Frost sine underkjelder blir normaliserte til stasjons-ID.

NVE sender nøkkelen i `X-API-Key`. HydAPI-seriar blir mappa til felles parameterar. Metadatajobben hentar òg persentilar og prøver å hente flaumstatistikk frå Chartserver. Sjå [klassifiseringa](nve_discharge_classification.md) for tydinga av klassane.

## Snower

Klienten brukar den konfigurerte `snk_`-nøkkelen i `authentication-key`, saman med `domain-id`. Brukarnamn/passord og `/login` er ikkje del av implementasjonen.

Metadata blir henta via `monitors_list`, `monitor_active_status` og `monitor_location`. Berre aktive monitorar blir tekne med. Etter migrering frå eldre Snower-oppsett må `sync-metadata` køyrast for å lagre `permanent_id` i `provider_context`.

Synken hentar `last_reading`. Grafar brukar `readings_list`, med handtering av leverandøren sine avkortingsheaderar og inntil 20 pagineringsrundar. Historikk kan derfor vere kortare enn perioden klienten bad om. Avkortinga blir logga, men er ikkje eit eige felt i tidsserieresponsen.

`probe_temperature` er mappa til `air_temperature` i koden. Måleplasseringa må takast med i tolkinga av temperaturen. `snow_on_road` er ikkje mappa til snødjupn. Manglande sensorverdi blir ikkje gjort om til måleverdien `0`.

## Tilsig

Nøkkelen blir send i `X-W2O-Api-Key`. Klienten listar stasjonar via `/v1/station/all`, seriar via `/v1/dataseries/by-station-id/<id>` og målingar via `/v1/measurement/dataseries/<id>`.

Berre seriar med namn som identifiserer Snow Depth Mode (eller `snø`), og samsvarande type, blir aksepterte. `mm` blir delt på 10 for å gi `cm`; `cm` blir behalde. Ukjende einingar blir hoppa over. Device Temperature blir ikkje tolka som lufttemperatur.

Timejobben leitar 48 timar tilbake, men lagrar berre siste verdi per parameter. Han fyller ikkje inn alle mellomliggjande snømålingar. Tidsseriekall hentar historikken direkte i bolkar på sju dagar.

## Wyssen

Nøkkelen blir send som `apikey` i querystrengen. Unngå å lagre komplette utgåande URL-ar med nøkkel i loggar. Klienten brukar `/app/api/time-series/` og hentar historikk i bolkar på tre dagar.

Fire stasjonar med koordinatar er statisk konfigurerte i `WYSSEN_STATIONS`: Peparstein 1 (`9902`), Svandalsflona 1 (`11894`), Austmannalia 3 (`11886`) og Skredene 1 (`12707`). Timejobben hentar dei siste to timane for temperatur, vindfart og vindretning.

Klienten kjenner fleire rå målekodar, men det offentlege tidsserie-API-et eksponerer berre desse tre parameterane for Wyssen. Nedbørsmengd/-intensitet er ikkje omrekna til `precipitation_1h`, og vindkast med ukjend måleperiode er ikkje likestilt med Frost sine vindkastaggregat.

`Settings.wyssen_base_url` er førebels ikkje kopla til klientopprettinga. Sjå [kodegjennomgangen](review-2026-10-28.md) før du prøver å endre adressa via miljøvariablar.

## Skred AS / InfluxDB

Klienten sender `Authorization: Token ...` til `POST /api/v2/query`, med Flux-spørjing og annotert CSV som svar. Standardane i klienten er organisasjon `skred_as`, bucket `skred_api_vikafjell` og measurement `aws_vikafjell`. Berre base-URL og token er eksponerte gjennom miljøoppsettet.

Vikafjell (`cs001`) er statisk registrert med koordinatar og høgd 971,5 moh. Timejobben hentar dei siste to timane av temperatur, vindfart og vindretning. Historikk blir henta i bolkar på tre dagar.

Desse ekstra parameterane kan hentast gjennom tidsserie-API-et, men blir **ikkje lagra av timejobben eller eksporterte som eigne felt i kartlaga**:

| Parameter | Eining i klienten |
| --- | --- |
| `wind_speed_of_gust` | `m/s` |
| `wind_speed_fc_low`, `wind_speed_fc_high` | `m/s` |
| `snow_drift_low`, `snow_drift_high` | `g/m2/s` |
| `snow_drift_low_cumsum`, `snow_drift_high_cumsum` | `g/m2/10min` |

Einingane over er dei koden returnerer. `*_cumsum` blir levert som leverandørverdiar; proxyen reknar ikkje sjølv ut desse og nullstiller dei ikkje ved starten av førespurnaden. Klienten kjenner òg fukt og fleire temperaturfelt internt, men dei er ikkje eksponerte i den offentlege parameterkatalogen.

Skred-vindkast er eit eige felt og går ikkje inn i eksisterande `wind_gust`-maksimum for døgn eller sju døgn.
