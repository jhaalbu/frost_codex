# Datakjelder

Dette dokumentet skildrar integrasjonane i koden, med overflatetemperatur oppdatert 2026-11-23. Overflatetemperatur er kontrollert med levande Frost- og Snower-kall; dei andre kjeldene er ikkje verifiserte med produksjonskall i denne gjennomgangen.

| Kjelde / `provider` | `source_id` | Aktivering | Data i vanleg synk |
| --- | --- | --- | --- |
| MET Frost / `frost` | T.d. `SN18700` | `FROST_CLIENT_ID` | Luft- og overflatetemperatur, timesnedbør, snødjupn, vind og vindkast der stasjonen har desse |
| NVE / `nve_hydapi` | T.d. `84.21.0` | `NVE_HYDAPI_KEY` | Utvalde seriar, mellom anna vassføring, grunnvatn, snø og nedbør |
| Snower / `snower` | `SNWR-<permanent_id>` | `SNOWER_API_KEY` og, for CLI-synk, `SNOWER_DOMAIN_ID` | `snow_depth`, `probe_temperature` → `air_temperature`, `ir_temperature` → `surface_temperature` |
| Tilsig / `tilsig` | `TILSIG-<station_id>` | `TILSIG_API_KEY` | Berre godkjende Snow Depth Mode-seriar → `snow_depth` |
| Wyssen / `wyssen` | `WYSSEN-<station_id>` | `WYSSEN_API_KEY` | `MTA` → temperatur, `MWS` → vindfart, `MWD` → vindretning |
| Skred AS / `skred` | `SKRED-cs001` | `SKRED_API_TOKEN` | Temperatur, vindfart og vindretning frå Vikafjell |
| Netatmo / `netatmo` | `NETATMO-<MAC utan kolon>` | Klient-ID, client secret og refresh token med `read_station` | Utetemperatur, vindfart og vindretning; timesnedbør når regnmålar finst |

Koordinatar er nødvendige for å kome med i kartlaget. Ein registrert stasjon utan koordinatar kan framleis finnast via stasjonsdetaljar og tidsseriar. `has_*` skildrar kjende måleeigenskapar, ikkje ein garanti for ferske verdiar.

## Netatmo

Klienten brukar [Netatmo Weather API](https://dev.netatmo.com/apidocumentation/weather): `getstationsdata` med `get_favorites=true` hentar stasjonane kontoen har tilgang til, inkludert delte og favorittstasjonar. Ingen stasjons-ID-ar treng å leggjast inn manuelt. Kvar basestasjon blir éin stasjon i proxyen. ID-en `70:ee:50:00:00:01` blir til `NETATMO-70ee50000001`; den opphavlege ID-en og modulreferansane ligg i `provider_context`.

`NAModule1` leverer utetemperatur, `NAModule2` vind og `NAModule3` nedbør. Basestasjonen og ekstra innemodular (`NAModule4`) blir ikkje brukte som utetemperatur. Namnet kjem uendra frå kontoen og kan derfor innehalde «Indoor» sjølv om målingane kjem frå utemodulane. Koordinatar, land og høgd kjem frå basestasjonen sin `place`. Manglande verdiar gir ingen observasjon; gyldig null blir behalden. Kvar modul beheld sitt eige måletidspunkt.

Timejobben lagrar siste dashboard-verdiar for temperatur og vind. Vind i km/t blir delt på 3,6 til `m/s`. Vindkast blir lagra og kan hentast som `wind_speed_of_gust`, men blir ikkje rekna som Frost sine `wind_gust_10m` eller eksporterte som vindkastmaksimum i kartlaga. Fukt, lufttrykk, CO₂ og støy er ikkje mappa til den felles parameterkatalogen.

Grafane hentar `getmeasure` med `scale=1hour`, `optimize=false` og `real_time=true`, i bolkar på høgst sju dagar (under Netatmo si grense på 1024 punkt). Temperatur og vind blir leverte som Netatmo sine timesaggregat. Nedbør brukar `sum_rain` i faste UTC-timar, registrert ved slutten av perioden; dashboardet sin glidande timesverdi blir ikkje akkumulert. Timejobben hentar lukka nedbørsperiodar med to timars overlapp. Ingen automatisk historisk tilbakefylling blir utført for temperatur og vind.

Autentisering brukar Bearer-token og [OAuth-fornying](https://dev.netatmo.com/apidocumentation/oauth). Konfigurer `NETATMO_CLIENT_ID`, `NETATMO_CLIENT_SECRET` og `NETATMO_REFRESH_TOKEN`; `NETATMO_ACCESS_TOKEN` er valfri når refresh token fungerer. Dei fire stavevariantane som alt finst i det lokale oppsettet (`NETAMO_CLIENT_ID`, `NETATMO_SECRET_ID`, `NETAMO_ACCESS_TOKEN`, `NETAMO_REFRESH_TOKEN`) blir aksepterte. Kanoniske namn har prioritet.

Fornya access- og refresh-token blir lagra i ei eiga SQLite-fil, standard `.netatmo_tokens.db`. Fila inneheld løyndomar og er ignorert av Git. Bruk `NETATMO_TOKEN_FILE` med same absolutte sti for webarbeidarar og CLI-jobbar i produksjon. Ei skrivelås held tokenfornying samla på tvers av trådar og prosessar; nye token blir lagra før bruk. `.env` blir ikkje omskriven. Ved ny OAuth-godkjenning endrar du refresh token i miljøet og startar prosessane på nytt. Ikkje bruk same tokenpar frå andre installasjonar med kvar si tokenfil.

Køyr `sync-metadata` og deretter `run-hourly` for å ta kjelda i bruk. Ein vellukka metadatajobb skjuler tidlegare Netatmo-stasjonar som ikkje lenger blir returnerte. Gamle observasjonar blir behaldne etter vanlege retensjonsreglar. Feil i metadatahentinga skjuler ikkje stasjonane. Endra tilgangssett gir ny cachemappe, så eldre kartfiler ikkje omgår filteret. Dei vanlege latest-rutene er offentlege også for Netatmo-stasjonar.

Levande kontroll 2026-11-25 stadfesta to tilgjengelege stasjonar med ute- og vindmodul. Dronestasjon Skaphenger returnerte temperatur, vind og vindkast; Dronehenger Vikafjellet returnerte metadata, men ingen målingar i det kontrollerte sekstimarsvindauget. OAuth-fornying og lagring av nye token, metadata/timejobb mot database i minnet, alle fire latest-variantane og begge HTTP-tidsserierutene vart kontrollerte. Ingen vêrdata vart skrivne til den konfigurerte databasen. Kontoen har ingen regnmålar; nedbør er berre testa med lokale mockdata.

## Frost og NVE

Frost brukar HTTP Basic med klient-ID som brukarnamn og tomt passord. Metadata kjem frå `sources` og tilgjengelege tidsseriar; observasjonar kjem frå `observations`. Frost sine underkjelder blir normaliserte til stasjons-ID.

NVE sender nøkkelen i `X-API-Key`. HydAPI-seriar blir mappa til felles parameterar. Metadatajobben hentar òg persentilar og prøver å hente flaumstatistikk frå Chartserver. Sjå [klassifiseringa](nve_discharge_classification.md) for tydinga av klassane.

HBV-modellert vassføring blir i tillegg henta frå VEPS som eigen parameter `discharge_hbv`, med morgondagens prognose i alle latest-variantane. Sjå [HBV-oppsettet](nve_hbv.md).

## Snower

Klienten brukar den konfigurerte `snk_`-nøkkelen i `authentication-key`, saman med `domain-id`. Brukarnamn/passord og `/login` er ikkje del av implementasjonen.

Metadata blir henta via `monitors_list`, `monitor_active_status` og `monitor_location`. Berre monitorar med den boolske verdien `active_status: true` som metadatajobben klarer å hente, blir synlege i API-et. Jobben oppdaterer òg statusen for tidlegare lagra stasjonar. Når ein stasjon fell ut av den aktive lista, blir han skjult frå både kartlag og historikkruter. Målingane blir behaldne etter dei vanlege reglane for lagringstid.

Etter oppgradering må `init-db` køyrast før `sync-metadata` fyller status for eksisterande stasjonar. Snower-stasjonar med ukjend status er skjulte. Når den aktive lista endrar seg, brukar webappen ei tilsvarande cachemappe eller byggjer svaret frå databasen. Køyr `refresh-geojson-cache` for å byggje filene straks, eller lat neste timejobb gjere det.

Synken hentar `last_reading`. Grafar brukar `readings_list`, med handtering av leverandøren sine avkortingsheaderar og inntil 20 pagineringsrundar. Historikk kan derfor vere kortare enn perioden klienten bad om. Avkortinga blir logga, men er ikkje eit eige felt i tidsserieresponsen.

`probe_temperature` er mappa til `air_temperature` i koden. Måleplasseringa må takast med i tolkinga av temperaturen. `ir_temperature` er mappa til `surface_temperature` i både `last_reading` og `readings_list`. Feltnamnet er kontrollert mot Snower si [eiga webapp](https://app.snower.fi/) og levande API-responsar 2026-11-23. API-et leverer eininga `Celsius`, som blir normalisert til `degC`. `snow_on_road` er ikkje mappa til snødjupn. Manglande sensorverdi blir ikkje gjort om til måleverdien `0`.

Levande kontroll 2026-11-23 omfatta tre aktive stasjonar: Bjorli flypass, Ev6 Hatter og Ev6 Karlebotn. `last_reading` og seks timar med `readings_list` gav overflatetemperatur på alle tre (29 historikkpunkt totalt). Innlasting i ein database i minnet, latest/full/compact/FME, `has=surface_temperature`, sjudøgnsekstrem og begge HTTP-tidsserierutene vart kontrollerte. Latest lagrar måletid med sekundoppløysing; den direkte tidsserien beheld leverandøren si tidsoppløysing. Kontrollen endra ikkje prosjektet sin database eller produksjonsdata.

### Overflatetemperatur frå Frost og Snower

Felles parameternamn er `surface_temperature`, i `degC`. [Frost sin elementkatalog](https://frost.met.no/elements/v0.jsonld?ids=surface_temperature) stadfestar at dette er den gamle MET-koden `TSS`, med målehøgd 0 m og skildringa «snow surface or bare ground» (kontrollert med autentisert kall 2026-11-23). Snower brukar den infraraude sensoren `ir_temperature`.

Frost-metadata og timesynk inkluderer `surface_temperature`. Snower ber om `ir_temperature` saman med dei andre sisteverdiane; capability blir sett når observasjonar kjem inn. Manglande sensor/data gir ingen observasjon. Overflatetemperatur på 0 °C er gyldig, også på vegstasjonar. Det eigne nullfilteret for lufttemperatur blir ikkje brukt på overflatetemperatur.

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
