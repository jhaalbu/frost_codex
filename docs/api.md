# API-referanse for Respons

Referansen skildrar koden per 2026-10-28. Eksempla er illustrative og er ikkje produksjonsmålingar.

## Baseadresse og prefiks

| Oppstart | Prefiks for rutene i tabellen under |
| --- | --- |
| `flask --app app run` / `pythonanywhere_wsgi.py` | `/weather` som standard; kan endrast med `FROST_ROUTE_PREFIX` |
| `python -m frost_sync serve` / `frost_sync.web.create_app()` | Ingen prefiks |
| Eiga Flask-app | Prefikset brukt ved registrering av `create_blueprint()` |

Alle rutene under brukar `GET`. Med standard `app.py` blir til dømes `/api/parameters` til `/weather/api/parameters`. Dei fire `latest*.geojson`-rutene er opne, inkludert `?has=...`. Tidsseriar og andre dataruter, inkludert daily og historikk, krev `X-API-Key` med verdien frå `PROXY_API_KEY`. Manglande/feil nøkkel på ei verna rute gir `401`. Sjå [proxy-nøkkel og klientoppsett](proxy_auth.md). Health-rutene og CORS-`OPTIONS` er opne. `app.py` aktiverer CORS; den frittståande blueprint-appen gjer ikkje det.

## Endepunkt

| Rute | Bruk og respons |
| --- | --- |
| `/health` | `{"status":"ok"}`. Stadfestar at ruta svarar; testar ikkje database, synk eller leverandørar |
| `/api/parameters` | Array med `id`, `label`, `unit`, `style`, `element_ids` for tidsserieparameterar |
| `/api/stations/latest.geojson` | Fullt kartlag: éin feature per stasjon med siste verdiar, eigenskapar og tidspunkt |
| `/api/stations/latest.geojson?has=snow_depth` | Databasebygd filtrering på éin måleeigenskap; snø krev òg ein siste snøverdi |
| `/api/stations/latest.compact.geojson` | Mindre kartlag; fjernar mellom anna `null`, einingsfelt og capability-flagg |
| `/api/stations/latest.fme.geojson` | Redusert, stabilt feltskjema med `null`; boolske verdiar blir `1`/`0` |
| `/api/stations/latest.7d.geojson` | Kompakt lag med maksimum siste sju døgn, temperaturminimum og akkumulert nedbør |
| `/api/stations/daily.geojson` | Stabilt døgnskjema for sist fullførte rapporteringsdøgn |
| `/api/stations/daily.geojson?date=2026-10-26` | Rapporteringsdøgn frå kl. 06 den 26. til kl. 06 den 27. i Europe/Oslo |
| `/api/stations/history.geojson` | Éin feature per lagra observasjon; krev tidsrom |
| `/api/stations/<source_id>` | Stasjonseigenskapar, `capabilities`, parameterprofil og `latest` |
| `/api/stations/<source_id>/observations` | Lagra råhistorikk som JSON; krev tidsrom |
| `/api/stations/<source_id>/discharge-thresholds` | Lagra NVE-tersklar `QM`, `Q5`, `Q50` og metadata; elles `404` |
| `/api/stations/<source_id>/timeseries` | Grafdata direkte frå leverandøren; krev tidsrom |
| `/api/timeseries` | Grafdata for fleire stasjonar; krev `stations` og tidsrom |

`has` gjeld berre fullversjonen av latest. Bruk eitt logisk parameternamn, til dømes `air_temperature` eller `snow_depth`. Dette er ikkje eit kommaseparert fleirfilter. Ukjent filter gir tomt lag.

GeoJSON brukar `FeatureCollection` med `Point`-geometri og koordinatrekkjefølgje **[lengdegrad, breiddegrad]**. Stasjonar utan koordinatar kjem ikkje med. Latest krev ei `station_latest`-rad og har ekstra kontrollar for mistenkelege NVE-verdiar. Sjudøgnslaget krev minst eitt berekna aggregat.

### Val av eksportvariant

Full latest er utgangspunktet for feltutforsking og feilsøking. Compact fjernar capability-flagg, koordinatkopiar i properties, einingsfelt og alle `null`-verdiar. Koordinatane ligg framleis i geometrien.

FME-varianten beheld `null` for eit stabilt skjema, men fjernar einingsfelt, koordinatkopiar, `country`, `county`, `municipality`, `last_seen_at`, `valid_to`, `parameter_profile`, `available_parameter_count`, `has_wind_gust_10m` og detaljerte `discharge_*`-felt bortsett frå `discharge_class` og `discharge_observed_at`. `has_discharge` er med. Nedbørsmaksimum brukar `precipitation_1h_max_time` og `precipitation_3h_max_time` som sluttid; full latest beheld `*_max_period`.

Sjudøgnslaget brukar mellom anna `air_temperature_max_7d`, `air_temperature_min_7d`, `snow_depth_max_7d`, `wind_speed_max_7d`, `wind_gust_max_7d`, `discharge_max_7d` og `groundwater_level_max_7d`, med tilhøyrande `*_time`. `precipitation_7d_accumulated` er sum over vindauget; `precipitation_24h_max` med `precipitation_24h_max_time` er største rullande døgnsum. Vindretninga ved maksimal vindfart heiter `wind_direction_at_7dmax`.

## Tid og parameterar

Historikk og tidsseriar krev anten `date=YYYY-MM-DD` eller både `from` og `to` som ISO 8601. Dersom `date` er sett, har han prioritet. Her tyder `date` eit **UTC-kalenderdøgn**; dette er ulikt `daily.geojson`, som brukar kl. 06–06 i Europe/Oslo. Tidsstempel utan tidssone blir tolka som UTC. Send helst `Z`; eit pluss i ein UTC-offset må URL-kodast som `%2B`.

`parameters` er ei kommaseparert liste frå `/api/parameters`. Utan parameterlista ber ruta om alle katalogparameterane. Vel eksplisitt dei som trengst for grafen. Ein parameter kan stå i katalogen sjølv om han ikkje finst på ein bestemt stasjon.

`stations` er ei kommaseparert liste med `source_id`; `source_ids` er eit alias. Duplikat blir fjerna. Leverandør blir slått opp frå databasen. Frost og NVE blir henta i grupper per leverandør, medan dei andre blir henta per stasjon.

```text
/weather/api/stations/SKRED-cs001/timeseries?parameters=air_temperature,snow_drift_low,snow_drift_high&from=2026-10-26T00:00:00Z&to=2026-10-27T00:00:00Z
/weather/api/timeseries?stations=SKRED-cs001,WYSSEN-9902&parameters=wind_speed,wind_from_direction&date=2026-10-26
```

## Tidsserierespons

`discharge_hbv` kan plottast saman med `discharge`. HBV blir henta frå VEPS og er modellert døgnmiddel, også for framtidige tidspunkt. Alle latest-variantane har HBV-prognosen for neste døgn med eige prognosetidspunkt og hentetid. Sjå [HBV-kontrakten og døme](nve_hbv.md) for felt, døgnval og utrulling. Desse HBV-felta blir behaldne òg i FME og dei kompakte eksportane.

Eitt-stasjonsruta returnerer denne forma (illustrativt datapunkt):

```json
{
  "station": {
    "provider": "skred",
    "source_id": "SKRED-cs001",
    "stationholder": "Skred AS",
    "name": "Vikafjell",
    "masl": 971.5
  },
  "from": "2026-10-26T00:00:00Z",
  "to": "2026-10-27T00:00:00Z",
  "series": {
    "air_temperature": {
      "parameter": "air_temperature",
      "unit": "degC",
      "data": [
        {"time": "2026-10-26T00:00:00Z", "timestamp": 1792972800000, "value": -2.4, "quality_code": null}
      ]
    }
  }
}
```

`timestamp` er Unix-tid i **millisekund**. `data: []` tyder at ingen punkt vart returnerte for parameteren, til dømes ved manglande sensor eller datadekning. Det tyder ikkje måleverdien null. `quality_code: null` tyder at kvalitetskode ikkje er oppgitt.

Fleirstasjonsruta returnerer `from`, `to`, `parameters`, `stations`, `missing` og `errors`. Kvart element i `stations` har `station` og `series` med same struktur som over. Ukjende ID-ar står i `missing`. Ved handterte kjeldefeil blir stasjonen utelaten frå `stations` og ført i `errors` med `source_id` og `message`. HTTP `200` kan derfor innehalde eit delvis resultat; klienten må lese desse listene.

For `discharge` inneheld serien òg `thresholds` (eller `null`) og `percentiles` (eventuelt tom liste). Tersklane har `qm`, `q5`, `q50`, `unit`, `series_version`, `updated_at`. Persentilpunkt har `date`, `time`, `timestamp`, `date_mmdd`, `mean`, `perc25`, `perc60`, `perc75`, `perc90`, `perc95`.

Frost prioriterer timesaggregat, med tilbakefall til råmålingar som blir aggregerte til timar. Andre kjelder kan ha finare oppløysing. `precipitation_24h_rolling` blir summert frå tilgjengelege timesverdiar med ekstra tilbakeblikk. `precipitation_accumulated` summerer gjennom det etterspurde tidsrommet. Ingen av desse responsane garanterer komplett timedekning.

## Ferskheit og tolking i kartportalen

| Felt / tilstand | Tyding |
| --- | --- |
| `observed_at` | Nyaste mappa observasjon på stasjonen; parameterane kan ha ulik alder |
| `*_observed_at` | Tidspunkt for den aktuelle måleverdien; bruk dette i ferskheitsvurderinga |
| `updated_at` | Lokal oppdatering/berekning av latest-rada, ikkje måletid |
| `minutes_since_observation`, `has_recent_data` | Alder og 120-minuttsgrense berekna då respons/cache vart bygd |
| `has_*` | Kjend måleeigenskap, ikkje bevis på noverande verdi |
| `null` / manglande felt i kompakt lag | Manglande/filtrert verdi; skal ikkje visast som `0` |

Latest-cachefiler blir berre serverte når endringsdatoen er same norske kalenderdato som førespurnaden; elles blir responsen bygd frå databasen. Dette hindrar at gårsdagens «neste døgn»-prognose blir vist som morgondagens. Innanfor same dato blir ikkje ferskheitsfelta rekna på nytt ved kvart kall. Respons bør berekne aktuell alder frå måletidspunkt og vise manglande/for gamle verdiar tydeleg. Sjudøgnslaget utelèt `has_recent_data`.

`daily_id` har forma `source_id_YYYYMMDD` og kan brukast ved FME append/update. Døgnet følgjer sommartid og kan ha 23 eller 25 timar. Sjudøgnsvindauget er relativt til byggetid for cachefila; utan cache er det relativt til førespurnaden.

Nedbørssummar og maksimum byggjer på tilgjengelege, godkjende målingar. Det finst ikkje eit fullstendigheitsfelt som stadfestar at alle timane er med. Grafdata brukar heller ikkje alle dei stasjonsspesifikke filtreringane som kartdata gjer, mellom anna nedbørsgrensa på 5 mm og temperaturfilter for vegstasjonar. Sjå [datametodane](data_methods.md).

## Cache og feil

Latest-variantane og siste fullførte daily-periode blir normalt serverte frå fil. `X-GeoJSON-Cache` er `file`, `file-gzip` eller `database-fallback`. `Accept-Encoding: gzip` kan gi ferdigkomprimert fil. Autentiserte dataresponsar får `Cache-Control: private, no-store`; serveren brukar framleis dei lokale cachefilene etter godkjend nøkkelkontroll. `has`-filter og eksplisitt daily-dato går direkte til databasen.

| Status | Typisk årsak |
| --- | --- |
| `401` | Manglande eller feil proxy-nøkkel i `X-API-Key` |
| `400` | Manglande tidsrom/stasjonsliste, ukjend parameter, ugyldig dato på daily-ruta |
| `404` | Ukjend stasjon eller manglande NVE-tersklar |
| `502` | Handtert `RuntimeError` frå direkte eitt-stasjons tidsseriekall |
| `500` | Uhandtert feil; mellom anna er ikkje all ugyldig tidsinput eller alle transportfeil fanga opp |

Nøkkelavvising (`401`) er JSON. Andre feilformat er ikkje einsarta JSON; `abort()` brukar Flask sitt standardformat. API-et har ikkje full validering av rekkjefølgja eller lengda på tidsrom. Sjå [oppfølgingspunkta](review-2026-10-28.md).

## Eldre Frost-proxyruter i app.py

Desse ligg utanfor `/weather` og finst ikkje i `python -m frost_sync serve`:

- `/frost?sources=...&elements=...&referencetime=...`: forenkla Frost-observasjonar, cache 600 sekund. Kvar post har `timestamp` og element med `value`, `resolution`, `offset`, `qualityCode`. Posten manglar stasjons-ID; unngå å blande fleire stasjonar når identiteten må bevarast.
- `/frost_available?sources=...`: liste over element-ID-ar, cache 3600 sekund.
- `/sources?sources=...`: Frost sin kjeldemetadatarespons, cache 600 sekund.

Desse rutene brukar ikkje dei lokale kartberekningane eller kvalitetsfiltra.
