# HBV-vassføring i Respons

`discharge_hbv` er døgnmiddel av modellert vassføring frå NVE VEPS, med datakjelde `hf_hbv` og element `6004` (`sim_discharge`). Eininga er `m3/s`. Dette er HBV-varianten som vart identifisert i seNorge-plottet for Storeskar. Sjå [undersøkingsnotatet](nve_hbv_exploration.md) og [VEPS-dokumentasjonen](https://vepsapi.nve.no/TimeSeries.md).

## Plotte modell og observasjon saman

Bruk dei to parameterane i same førespurnad, med eit tidsrom som strekkjer seg fram til dei ønskte prognosedatoane:

```text
/weather/api/stations/12.215.0/timeseries?parameters=discharge,discharge_hbv&from=2026-10-27T00:00:00Z&to=2026-10-31T00:00:00Z
```

Det same fungerer i `/weather/api/timeseries?stations=12.215.0,...&parameters=discharge,discharge_hbv&from=...&to=...`.

- `series.discharge` inneheld observasjonsbasert vassføring frå HydAPI, med eksisterande terskel- og persentilmetadata.
- `series.discharge_hbv` inneheld modellert døgnmiddel frå VEPS, inkludert framtidige punkt innanfor førespurt tidsrom. Serien har `data_type: "model"`, `model: "hf_hbv"`, `resolution: "P1D"` og `time_offset: "PT6H"`.
- Punkta har same format som andre grafseriar: `time`, `timestamp` i millisekund, `value` og `quality_code`. Modellpunkta har `quality_code: null`; dette er ikkje ei kvalitetsgodkjenning.
- Stasjonar utan HBV-data får `data: []`. VEPS-feil gir eksisterande feilhandsaming for tidsseriar (`502` ved handtert feil på enkeltstasjon, eller `errors` ved fleirstasjonskall).
- Ingen flaumklassifisering eller tersklar blir lagde på modellserien. Døgnmiddel og observert momentanvassføring har ulikt tidsgrunnlag.

Modellkøyringar blir valde med eit vidt `generatedTime`-intervall, slik seNorge gjer. Same historiske førespurnad kan derfor få reviderte modellverdiar seinare; API-et eksponerer førebels ikkje ein fast modellkøyring.

## Neste døgn i alle latest-variantane

Timejobben hentar HBV for dei eksisterande `nve_hydapi`-stasjonane i databasen, i grupper på 25. VEPS svarar berre med data der modelldekning finst. Det blir ikkje oppretta eigne kopiar av stasjonane.

**«Neste døgn» er modellpunktet med VEPS-referansetid kl. 06 UTC på morgondagens dato, rekna i Europe/Oslo.** Det er ikkje maksimum neste 24 timar eller gjennomsnitt rekna frå tidspunktet du kallar API-et. Proxyen beheld referansetida frå VEPS og påstår ikkje at ho er starten eller slutten på berekningsdøgnet. Dette er heller ikkje same tidsgrunnlag som det lokale kl. 06–06-døgnet i `daily.geojson`.

Følgjande felt blir eksporterte i `latest.geojson`, `latest.compact.geojson`, `latest.fme.geojson` og `latest.7d.geojson`, og i `latest` på stasjonsdetaljen:

| Felt | Tyding |
| --- | --- |
| `discharge_hbv` | Modellert døgnmiddel for neste døgn, i m³/s |
| `discharge_hbv_forecast_time` | VEPS-referansetid for modellpunktet, ISO 8601 UTC |
| `discharge_hbv_fetched_at` | Når proxyen henta verdien; ikkje tidspunkt for modellkøyringa |
| `discharge_hbv_unit` | `m3/s` |
| `discharge_hbv_model` | `hf_hbv` |
| `discharge_hbv_resolution` | `P1D` |
| `discharge_hbv_time_offset` | `PT6H` |
| `has_discharge_hbv` | Om eit gyldig modellpunkt for morgondagen finst ved responsbygging |

FME beheld alle HBV-felta, inkludert eininga, og kodar flagget som `1`/`0`. Dei kompakte variantane beheld HBV-metadata og eininga som eit unntak frå den vanlege feltreduksjonen. Manglande modellverdi gir `null` i full latest/FME og utelatne måle-/metadatafelt i compact/7d; `has_discharge_hbv` blir `false` (eller `0` i FME). Flagget skildrar tilgjengeleg prognose, ikkje permanent sensorkapasitet, og inngår ikkje i `parameter_profile` eller `available_parameter_count`.

Bruk `/weather/api/stations/latest.geojson?has=discharge_hbv` for berre stasjonar som har eit gyldig modellpunkt for morgondagen.

I `latest.7d.geojson` er `discharge_hbv` framleis morgondagens prognose, ikkje eit sjudøgnsaggregat. Ein stasjon med gyldig HBV-prognose kan vere med sjølv om han manglar historiske sjudøgnsaggregat. `daily.geojson` og observasjonshistorikken blir ikkje utvida med prognoseverdiar.

## Ferskheit, manglande data og lagring

Modellverdien blir lagra i eigne kolonnar i `station_latest`. Han blir ikkje lagra i `observations`, endrar ikkje `observed_at`, `last_observation_time`, observasjonsfelta eller deira ferskheit, og inngår ikkje i observerte maksimum, nedbørssummar eller flaumklassar.

Etter eit vellukka VEPS-kall blir tidlegare modellverdi tømd dersom akkurat morgondagens punkt manglar. Proxyen vel ikkje ein annan prognosedato som erstatning. Ved VEPS-feil blir førre vellukka verdi behalden med opphavleg `discharge_hbv_fetched_at`, og feilen blir logga/tald i `station_errors`. Kvar gruppe blir lagra i eigen transaksjon etter at observasjonane er committa.

Ved ny responsbygging blir ein modellverdi undertrykt dersom referansetida ikkje lenger gjeld morgondagen. Ferdigbygde latest-cachefiler frå ein annan norsk kalenderdato blir forbigått, slik at databaseresponsen kan fjerne slike prognosar. HTTP-cachetida blir avgrensa til neste norske midnatt. Innanfor same dato kan filene framleis vere gamle dersom timejobben stoppar; vis derfor alltid prognosedato og hentetid i Respons.

## Oppsett og utrulling

HBV blir aktivert saman med NVE-integrasjonen (`NVE_HYDAPI_KEY`). Nøkkelen blir berre brukt mot HydAPI; VEPS har eiga sesjon og krev ikkje nøkkel i denne integrasjonen. `NVE_VEPS_BASE_URL` kan overstyre standardadressa `https://vepsapi.nve.no`.

Etter at koden er lagt ut, køyr:

```bash
python -m frost_sync init-db
python -m frost_sync run-hourly
```

`init-db` legg til dei tre kolonnane `discharge_hbv`, `discharge_hbv_forecast_time` og `discharge_hbv_fetched_at` i eksisterande database. Køyr `sync-metadata` før timejobben dersom stasjonane ikkje er registrerte frå før. Timejobben byggjer òg cachefilene på nytt. Last webappen på nytt etter kode-/skjemaoppdateringa.

Ved innbygging av `SyncService` i eigen kode må VEPS-klienten sendast som `veps_client=...`; den vanlege CLI-en koplar dette automatisk til `nve_hydapi_client.veps_client`.
