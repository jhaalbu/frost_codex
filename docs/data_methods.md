# Datametodar og filter

Dette dokumentet skildrar handsaminga av lagra observasjonar og GeoJSON-eksportar. Sjå [API-referansen](api.md) for klientkontrakten. Direkte tidsseriar brukar ikkje alle dei stasjonsspesifikke filtra som blir brukte på lagra kartdata; sjå API-referansen før du samanliknar dei to.

Sjå [NVE-klassifisering og vassføringskurver](nve_discharge_classification.md) for omtale av vassføringskurver i HydAPI, persentilar og klassifiseringsfelt til symbolisering i ArcGIS/VertiGIS.

### Datakjelder

Sjå [Datakjelder](providers.md) for alle dei seks leverandørane og korleis data frå dei blir mappa til felles felt.

### Kvalitetsfilter

- Førespurnader til Frost brukar `FROST_QUALITY_CODES`, med standardverdien `0,1,2,3,4`.
- Observasjonar med `quality_code >= 5` blir ikkje lagra når kvalitetskode er oppgitt.
- `prune-db` slettar òg lagra observasjonar med `quality_code >= 5`.
- Kvalitetsverdiar frå NVE og Snower blir lagra når dei er tilgjengelege. Manglande kvalitet er tillate, sidan ikkje alle leverandørresponsar brukar same modell for kvalitetskodar.

### Siste verdiar

- `station_latest` lagrar éi flat rad per stasjon til kartvising.
- `observed_at` er observasjons-/referansetidspunktet frå leverandøren for det nyaste mappa elementet i rada. Feltet blir behalde som siste tidspunkt for heile stasjonen, av omsyn til kompatibilitet og generell vurdering av ferskheit.
- Kvar primær måleverdi har òg sitt eige tidsstempel: `air_temperature_observed_at`, `precipitation_observed_at`, `snow_depth_observed_at`, `wind_speed_observed_at`, `wind_from_direction_observed_at`, `discharge_observed_at` og `groundwater_level_observed_at`. Bruk desse når alderen på ein bestemt verdi er viktig; parameterar på same stasjon kan bli oppdaterte til ulike tider.
- `updated_at` er UTC-tidspunktet då den lokale `station_latest`-rada sist vart endra eller dei avleidde verdiane vart rekna ut på nytt. Feltet skildrar lokal handsaming, ikkje måletidspunktet, og kan derfor vere seinare enn `observed_at`.
- `minutes_since_observation` blir ikkje lagra i databasen. Feltet blir rekna ut når GeoJSON-responsen eller cachefila blir bygd, som talet på heile minutt mellom gjeldande UTC-tid og `stations.last_observation_time`, som normalt svarar til `observed_at`. Verdien er `null` når måletidspunktet er ukjent.
- `has_recent_data` blir rekna ut frå same måletidspunkt og er `true` når `minutes_since_observation` er høgst 120 minutt.
- API-et returnerer tidsstempel i ISO 8601-format med UTC, til dømes `2026-08-05T12:00:00Z`.
- Når `init-db` blir køyrd på nytt, legg kommandoen til manglande tidsstempelkolonnar for kvar parameter og fyller dei frå dei nyaste samsvarande radene som er behaldne i `observations`. Tidsstempelet blir verande `null` når det ikkje finst ei samsvarande historikkrad, og blir fylt ved ein seinare synk.
- Felt som `has_air_temperature` og `has_snow_depth` kjem frå `station_capabilities`, ikkje frå om siste måleverdi er ulik `null`.
- `parameter_profile` blir avleidd frå måleeigenskapane: `complete` tyder temperatur, nedbør, vind og snø; `weather` tyder temperatur, nedbør og vind; `snow` tyder at snødata er tilgjengelege; alle andre kombinasjonar får `lesser`.

### Nedbørsfilter

- Timesnedbør frå Frost og NVE blir normalisert til det logiske feltet `precipitation_1h`.
- For NVE HydAPI-stasjonar blir `precipitation_1h < 0` og `precipitation_1h > 5 mm` rekna som mistenkjelege verdiar.
- For vegstasjonar frå SVV/Statens vegvesen blir `precipitation_1h > 5 mm` rekna som mistenkjeleg, sidan optiske sensorar kan rapportere for mykje under snøfokk.
- Mistenkjeleg nedbør blir utelaten frå siste nedbørsverdi og frå berekning av rullande summar og maksimum.
- `is_precipitation_suspect` blir sett når den siste nedbørsobservasjonen på ein stasjon vart fjerna av dette filteret.
- For `latest.7d.geojson` blir same strenge grense på 5 mm per time brukt for NVE og Vegvesen/SVV før maksimum og akkumulert nedbør blir rekna ut.
- Stasjonane `2.36.0` og `SN11000` er utelatne frå timesnedbør fordi dei over tid har rapportert verdiar som ikkje oppfører seg som reell timesnedbør.

### Snøfilter

- Frost sitt `surface_snow_thickness`, NVE sitt `snow_depth` og snødjupn frå Snower blir normaliserte til det logiske feltet `snow_depth`.
- Snøverdiar på `-1` blir normaliserte til `0`.
- Snøverdiar `<= -3` blir forkasta.
- For snødjupn frå NVE blir negative verdiar normaliserte til `0`, medan verdiar over `1000` blir forkasta som urealistiske.
- `snow_depth_change` samanliknar siste snødjupn med den næraste observasjonen rundt 24 timar tidlegare, med eit søkjevindauge på sju dagar.
- Dersom siste snødjupn er `0` og ingen snødjupn dei siste 24 timane er over `0`, blir `snow_depth_change` utelate.

### Temperaturfilter

- For SVV/Statens vegvesen-stasjonar blir lufttemperaturar frå `-40.5` til `-39.5` handsama som sensor-/feilkodar og utelatne frå siste verdiar og avleidde temperaturaggregat.
- Lufttemperaturar nær `0` blir utelatne frå `station_latest` når naboverdiar i dei kringliggjande seks timane gjennomgåande ligg minst fem grader frå null på same side, når mønsteret i dei kringliggjande 24 timane er dominert av nøyaktige nullverdiar medan fleire verdiar på same side ligg langt frå null, eller når dei siste seks timane er ei flat rekkje med nullverdiar. Dette fjernar einskilde og gjentekne sensorutfall utan å filtrere bort vanleg vêr rundt frysepunktet.
- Dersom den nyaste råverdien for lufttemperatur blir filtrert som mistenkjeleg, blir `latest.air_temperature` sett til `null` i staden for å bruke ein eldre gyldig temperatur.

### HBV-modellert vassføring

`discharge_hbv` er eit eige VEPS-modellfelt med døgnmiddel for neste døgn i alle latest-variantane og direkte tidsseriar til graf. Det blir lagra separat frå observasjonane og inngår ikkje i observerte aggregat eller flaumklassifisering. Sjå [HBV-metode og døgnval](nve_hbv.md).

### Klassifisering av vassføring

- Vassføring frå NVE HydAPI blir normalisert til det logiske feltet `discharge`.
- `sync-metadata` hentar HydAPI `Percentiles` for NVE-stasjonar med vassføringsseriar og lagrar dei i `nve_discharge_percentiles`.
- `sync-metadata` brukar òg versjonen av vassføringsserien frå HydAPI til å hente Chartserver-statistikkane `QCm`, `QC5` og `QC50`. Desse blir lagra som tersklane `discharge_flood_qm`, `discharge_flood_q5` og `discharge_flood_q50`.
- Latest-endepunkta for GeoJSON klassifiserer siste vassføring mot persentilrada for observasjonsdatoen.
- Flaumtersklar har prioritet framfor persentilar: verdiar på eller over `Q50` får `flood_over_50y`, verdiar på eller over `Q5` får `flood_5y_to_50y`, og verdiar på eller over `QM` får `flood_mean_to_5y`. Verdiar under `QM`, eller stasjonar utan flaumtersklar, brukar persentilklassifiseringa.
- Chartserver-feil og manglande serieversjonar blir handterte per stasjon; tidlegare lagra tersklar og persentilklassifisering er framleis tilgjengelege.
- Persentilklassane er `low`, `normal`, `high_minus`, `high` og `high_plus`.
- Persentilbanda er `< P25 = low`, `P25–P60 = normal`, `P60–P75 = high_minus`, `P75–P90 = high` og `>= P90 = high_plus`. Flaumklassane overstyrer desse banda når `QM`, `Q5` eller `Q50` er nådd.
- Denne persentilklassifiseringa er ikkje det same som dei offisielle tersklane for flaumvarsling frå NVE.
- Køyr `init-db` éin gong etter utrulling av denne funksjonen, slik at tabellane for NVE-persentilar og flaumtersklar finst. Køyr deretter `sync-metadata` for å fylle dei.

### Avleidde felt for siste 24 timar

- `precipitation_24h` er summen av timesnedbørsrader dei siste 24 timane fram til siste måletidspunkt på stasjonen.
- `precipitation_1h_max` er den største godkjende timesnedbørsverdien i same 24-timarsvindauge.
- `precipitation_1h_max_period` lagrar éintimesperioden då maksimumsverdien vart målt.
- `precipitation_3h` er ein rullande tretimarssum som sluttar ved siste måletidspunkt på stasjonen.
- `precipitation_3h_max` er den største rullande tretimarssummen dei siste 24 timane.
- `precipitation_3h_max_period` lagrar tretimarsperioden for denne maksimumssummen.
- `air_temperature_min` og `air_temperature_max` er lågaste og høgaste temperatur i siste 24-timarsvindauge. `air_temperature_min_time` og `air_temperature_max_time` lagrar tidspunkta for desse ekstremverdiane.
- `wind_speed_max` er den største vindfarten i siste 24-timarsvindauge, og `wind_from_direction_max` er vindretninga på nøyaktig same tidspunkt når ho er tilgjengeleg.

### Metode for sjudøgns-GeoJSON

- `latest.7d.geojson` blir rekna ut frå lagra `observations`, ikkje frå kolonnane i `station_latest`.
- Innkomande observasjonar med tidsstempel seinare enn gjeldande UTC-tid blir avviste. Lufttemperaturar utanfor det inklusive intervallet −60 til +60 grader Celsius blir òg avviste.
- Eksportane filtrerer òg bort slike ugyldige historiske observasjonar. Ugyldige mellomlagra sisteverdiar og framtidige måletidspunkt blir undertrykte; observasjonshistorikken blir ikkje sletta. Etter utrulling av denne endringa: køyr `python -m frost_sync refresh-geojson-cache` for å erstatte eksisterande eksportfiler, og last webappen på nytt. Den vanlege timejobben byggjer òg cachefilene på nytt.
- Tidsvindauget er dei siste sju døgna rekna frå tidspunktet då responsen blir bygd: tidspunktet for cachebygging ved filrespons, eller tidspunktet for førespurnaden ved tilbakefall til databasen.
- Responsen inneheld éin feature per stasjon når minst eitt sjudøgnsaggregat eller ein gyldig HBV-prognose for morgondagen finst.
- Ved vanleg drift byggjer `run-hourly` dette endepunktet på førehand som ei cachefil. Webruta byggjer det berre på nytt frå databasen dersom cachefila manglar.
- Maksimumsfelta har namn som `air_temperature_max_7d`, `snow_depth_max_7d`, `wind_speed_max_7d`, `wind_gust_max_7d`, `discharge_max_7d` og `groundwater_level_max_7d`.
- Kvart maksimumsfelt har òg eit tidsstempelfelt med namnet `*_max_7d_time`.
- Lufttemperatur har i tillegg `air_temperature_min_7d` og `air_temperature_min_7d_time`.
- `wind_direction_at_7dmax` er vindretninga målt på nøyaktig same tidspunkt som `wind_speed_max_7d`. Feltet blir utelate dersom det ikkje finst ei samsvarande retningsmåling.
- For vassføring beheld sjudøgnsendepunktet berre `discharge_max_7d`, `discharge_max_7d_time` og `discharge_class`. Klassen blir rekna ut frå `discharge_max_7d`, med døgnpersentilane for `discharge_max_7d_time` og dei lagra flaumtersklane.
- `latest.7d.geojson` utelèt `has_recent_data`; `minutes_since_observation` er framleis tilgjengeleg.
- `precipitation_7d_accumulated` er summen av godkjende timesnedbørsverdiar i sjudøgnsvindauget.
- `precipitation_24h_max` er den største rullande 24-timars nedbørssummen med sluttid innanfor sjudøgnsvindauget, og `precipitation_24h_max_time` er denne sluttida. Berekninga les 24 timar ekstra før sjudøgnsvindauget, slik at dei første rullande vindauga er komplette. Dette tilbakeblikket inngår ikkje i `precipitation_7d_accumulated` eller andre sjudøgnsmaksimum.
- Sidan endepunktet les frå `observations`, trengst minst sju dagar med lagra observasjonar for dei vanlege sjudøgnsaggregata. Bruk minst åtte dagar for `precipitation_24h_max`, som òg treng dei føregåande 24 timane. Lagringstid åleine garanterer ikkje komplett måledekning og fyller ikkje inn manglande historikk.

### Metode for døgn-GeoJSON

- `daily.geojson` er meint for dagleg innlegging eller oppdatering via FME i eit ArcGIS-historikklag. Responsen inneheld éin feature per stasjon med ein stabil `daily_id` på forma `source_id_YYYYMMDD`.
- Eit rapporteringsdøgn går frå kl. 06:00 på `period_date` fram til, men ikkje med, kl. 06:00 neste dag i tidssona `Europe/Oslo`. Dette blir handtert korrekt ved overgang mellom sommartid og normaltid. `period_start` og `period_end` blir returnerte som UTC-tidsstempel.
- Utan datoparameter returnerer endepunktet siste fullførte kl. 06–06-periode og brukar normalt den ferdigbygde cachefila. Bruk `?date=YYYY-MM-DD` for å byggje ein bestemt periode som framleis er lagra, direkte frå databasen.
- Skjemaet er stabilt: felt utan observasjonar blir behaldne som `null`, slik at responsen passar til FME og eit fast feltskjema i ei ArcGIS-featureteneste.
- Temperatur har døgnminimum og -maksimum med tidsstempel. Andre måletypar har berre maksimumsverdiar med tidsstempel. Vindretning blir teken med på nøyaktig tidspunkt for maksimal vindfart når ho er tilgjengeleg.
- `precipitation_24h` er summen av godkjend timesnedbør i rapporteringsperioden. Endepunktet inneheld òg største timesnedbør og største rullande tretimarsnedbør med sluttid for akkumuleringa.
- `discharge_class` blir rekna ut frå `discharge_max`, med persentilar for datoen for maksimumet og lagra tersklar for `QM`, `Q5` og `Q50`.
- Endepunktet les frå observasjonar som framleis finst i `observations`. Ein historisk dato som er eldre enn `FROST_RETENTION_DAYS`, kan derfor ha målefelt med `null`.

### Metode for kompakt GeoJSON

- `latest.compact.geojson` brukar same stasjonsfiltrering som `latest.geojson`, men fjernar flagg for måleeigenskapar, koordinatfelt i properties, einingsfelt og alle nullverdiar.
- `latest.7d.geojson` brukar same kompakte utforming, slik at laget kan leggjast direkte til eit ArcGIS-webkart med mindre datamengd.
- Begge dei kompakte endepunkta beheld koordinatar berre i GeoJSON-geometrien.
- Begge blir serverte frå ferdigbygde filer når filene finst i `FROST_GEOJSON_CACHE_DIR`.
- Webresponsen inneheld `X-GeoJSON-Cache`; venta verdiar er `file-gzip`, `file` eller `database-fallback`.

### Metode for tidsseriar

- `/api/stations/<source_id>/timeseries` og `/api/timeseries` hentar grafdata direkte frå leverandøren i staden for å lese historiske observasjonar frå den lokale databasen.
- API-et slår opp leverandøren frå `source_id`, så klienten treng ikkje sende leverandør i førespurnaden.
- Førespurnader til Frost og NVE blir samla per leverandør for `/api/timeseries`.
- Frost-tidsseriar prioriterer timeselement som `mean(air_temperature PT1H)`, `mean(wind_speed PT1H)`, `mean(wind_from_direction PT1H)` og `max(wind_speed_of_gust PT1H)`.
- Dersom dei prioriterte timeselementa ikkje er tilgjengelege frå Frost, blir råverdiar henta og aggregerte til timepunkt i API-responsen.
- `precipitation_24h_rolling` blir avleidd frå leverandøren sine timesnedbørsverdiar.
- `precipitation_accumulated` summerer nedbør gjennom det etterspurde tidsrommet.

### Lagringstid og planlagde jobbar

- `run-hourly` oppdaterer observasjonar og `station_latest`, men slettar ikkje gamle observasjonar.
- `run-hourly` oppdaterer òg cachefilene `latest.geojson`, `latest.fme.geojson`, `latest.compact.geojson`, `latest.7d.geojson` og `daily.geojson` etter at databasetransaksjonen er stadfesta (commit).
- `sync-metadata` oppdaterer stasjonsmetadata og måleeigenskapar og bør køyrast dagleg eller når leverandøroppsettet blir endra.
- `prune-db` slettar observasjonar som er eldre enn `FROST_RETENTION_DAYS`, og rader med dårleg kvalitet.
- Standard lagringstid er 14 dagar. Dette er nok for sisteverdiar med 24-timarsaggregat og sjudøgnsendepunktet for GeoJSON.
- `refresh-geojson-cache` byggjer dei mellomlagra GeoJSON-filene på nytt, inkludert gzip-komprimerte `.gz`-variantar.
- `FROST_GEOJSON_CACHE_DIR` styrer kvar ferdigbygde GeoJSON-filer blir lagra. Relative stiar blir tolka ut frå prosjektroten.
