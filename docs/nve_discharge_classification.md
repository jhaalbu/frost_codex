# NVE: klassifisering av vassføring og vassføringskurver

Dette notatet skildrar korleis vassføringskurver frå HydAPI kan brukast i appen, og kva datafelt som bør eksponerast for symbolisering i ArcGIS/VertiGIS.

For modellert vassføring i seNorge, sjå [utforsking av HBV og VEPS for Storeskar](nve_hbv_exploration.md). Dette er ei eiga datakjelde, skild frå observasjonsseriane i HydAPI.

## Kva vassføringskurvene i HydAPI gir

HydAPI `Ratingcurves` listar vassføringsseriar som har tilgjengelege vassføringskurver.

For ein bestemt stasjon returnerer `GET /api/v1/Ratingcurves/{stationId}` éin eller fleire kurveperiodar. Kvar periode er avgrensa av `dtStartDate` og `dtEndDate`. Dersom `dtEndDate` er `null`, er perioden framleis aktiv.

Kvar periode kan innehalde eitt eller fleire segment. Eit segment gjeld frå `minimum` til `maximum`, og vassføringa blir rekna ut slik:

```text
Q = constant * (input - zero) ^ exponent
```

Formelen brukar desse verdiane:

- `input` er vasstand.
- `Q` er vassføring.
- `constant`, `zero` og `exponent` kjem frå det valde segmentet.

Det valde segmentet er det segmentet der vasstanden ligg innanfor `minimum` og `maximum`.

## Kva vassføringskurvene ikkje gir

Vassføringskurvene fortel ikkje om den aktuelle vassføringa er:

- på nivå med eller over 50-årsflaum
- mellom 5-årsflaum og 50-årsflaum
- mellom middelflaum og 5-årsflaum
- høg, normal eller låg

Desse klassane krev klassifiseringstersklar. Vassføringskurvene i HydAPI omreknar berre vasstand til vassføring. Appen hentar no tersklane frå NVE Chartserver-statistikk for vassføringsserien med rett versjon og lagrar dei lokalt.

HydAPI `Percentiles` kan bidra til å klassifisere normal, høg og låg vassføring dersom persentildata finst for stasjonen/parameteren. Persentilar er likevel skilde frå vassføringskurver.

## Framlegg til kartfelt

For å gjere symbolisering enkel bør GeoJSON eksponere eitt flatt felt for klassen og nokre støttefelt.

Tilrådde felt:

- `discharge`: siste vassføringsverdi i m³/s.
- `discharge_observed_at`: tidsstempel for siste vassføringsobservasjon.
- `discharge_age_hours`: alder på siste vassføringsobservasjon.
- `discharge_age_class`: `fresh`, `stale_4_24h`, `stale_over_24h` eller `missing`.
- `discharge_class`: namn på symbolklassen.
- `discharge_class_rank`: numerisk rang for sortering og symboliseringsreglar.
- `discharge_class_source`: `flood_threshold`, `percentile`, `rating_curve`, `latest_value_only` eller `none`.
- `discharge_classification_missing`: `true` når vassføring finst, men klassifiseringstersklar manglar.
- `discharge_value_missing`: `true` når vassføringsverdi manglar.
- `discharge_error`: valfri, kort feil-/statustekst.

Framlegg til verdiar for `discharge_class`:

- `flood_over_50y`
- `flood_5y_to_50y`
- `flood_mean_to_5y`
- `high_plus`
- `high`
- `high_minus`
- `normal`
- `low`
- `missing_value`
- `missing_classification`
- `other_error`

Framlegg til verdiar for `discharge_class_rank`:

- `90`: på nivå med eller over 50-årsflaum
- `80`: mellom 5-årsflaum og 50-årsflaum
- `70`: mellom middelflaum og 5-årsflaum
- `60`: høg pluss
- `50`: høg
- `40`: høg minus
- `30`: normal
- `20`: låg
- `10`: manglande klassifisering
- `0`: manglande verdi
- `-1`: annan feil

## Framlegg til klassifiseringsmetode

Bruk denne prioriteringa:

1. Dersom siste vassføring manglar, set `discharge_class = missing_value`.
2. Dersom stasjonen har flaumtersklar, klassifiser mot desse først.
3. Dersom flaumtersklar manglar, men persentilgrenser finst, bruk persentilklassifisering.
4. Dersom berre vassføring finst, set `discharge_class = missing_classification`.
5. Ved API- eller berekningsfeil, set `discharge_class = other_error`.

Klassifisering etter flaumtersklar:

- `discharge >= q50`: `flood_over_50y`
- `q5 <= discharge < q50`: `flood_5y_to_50y`
- `qm <= discharge < q5`: `flood_mean_to_5y`

Persentilbasert klassifisering som tilbakefall:

- `discharge >= perc90`: `high_plus`
- `perc75 <= discharge < perc90`: `high`
- `perc60 <= discharge < perc75`: `high_minus`
- `perc25 <= discharge < perc60`: `normal`
- `discharge < perc25`: `low`

Dette er ei praktisk persentilbasert klassifisering, ikkje det same som tersklane for flaumvarsling frå NVE.

## Bruk av vassføringskurver i appen

Vassføringskurver er særleg nyttige når:

- ein stasjon har vasstand, men ikkje direkte vassføring
- vi vil kontrollere vassføring mot vasstand
- vi vil vise om ein vassføringsverdi er målt direkte eller rekna ut frå vasstand

Framlegg til framtidige felt dersom vi reknar ut vassføring frå vasstand:

- `water_stage`
- `water_stage_observed_at`
- `rating_curve_available`
- `rating_curve_period_no`
- `rating_curve_segment_no`
- `rating_curve_valid_from`
- `rating_curve_valid_to`
- `discharge_calculated_from_stage`
- `discharge_calculation_error`

For dagens kartsymbolisering er direkte `discharge` frå HydAPI det enklaste og tryggaste utgangspunktet. Vassføringskurver kan leggjast til seinare for å fylle hol for stasjonar som berre eksponerer vasstand.

## Første implementeringssteg

Appen lagrar vassføringspersentilar frå HydAPI i `nve_discharge_percentiles` når `sync-metadata` blir køyrd. Latest- og compact-GeoJSON koplar persentilrada for observasjonsdatoen til siste vassføringsverdi og eksponerer:

- `discharge_observed_at`
- `discharge_age_hours`
- `discharge_age_class`
- `discharge_class`
- `discharge_class_rank`
- `discharge_class_source`
- `discharge_classification_missing`
- `discharge_value_missing`
- `discharge_percentile_date`
- `discharge_perc25`
- `discharge_perc60`
- `discharge_perc75`
- `discharge_perc90`
- `discharge_perc95`

Dersom ein stasjon har vassføring, men manglar persentilrad for observasjonsdatoen og ikkje kan klassifiserast med flaumtersklar, får han:

```text
discharge_class = missing_classification
discharge_class_source = latest_value_only
discharge_classification_missing = true
```

Ved persentilklassifisering får stasjonen `discharge_class_source = percentile`.

## Implementering av flaumtersklar

For ein NVE-vassføringsserie som `84.21.0.1001.1` er `84.21.0` stasjons-ID, `1001` vassføringsparameteren og den siste `1` serieversjonen. Versjonen blir henta frå seriemetadata i HydAPI og blir ikkje teken for gitt.

Under `sync-metadata` hentar appen Chartserver-statistikkane `qcm`, `qc5` og `qc50` og lagrar éi rad i `nve_discharge_flood_thresholds` per stasjon. GeoJSON eksponerer:

- `discharge_flood_qm`
- `discharge_flood_q5`
- `discharge_flood_q50`
- `discharge_flood_unit`
- `discharge_flood_series_version`
- `discharge_flood_updated_at`

Flaumklassifisering har prioritet når siste vassføring når ein flaumterskel. Under `QM` blir døgnpersentilane frå HydAPI brukte som tilbakefall. Ein Chartserver-feil påverkar berre den aktuelle stasjonen og slettar ikkje ei tidlegare lagra terskelrad.

Merknader ved utrulling:

- Køyr `python -m frost_sync init-db` éin gong etter utrulling av denne funksjonen, slik at den nye tabellen `nve_discharge_percentiles` finst.
- Køyr `python -m frost_sync sync-metadata` for å fylle eller oppdatere NVE-persentilradene.
- Køyr deretter `python -m frost_sync run-hourly` eller `python -m frost_sync refresh-geojson-cache`, slik at dei mellomlagra GeoJSON-filene inneheld dei nye felta.
