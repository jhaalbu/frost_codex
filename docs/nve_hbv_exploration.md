# Utforsking av NVE HBV-data for Storeskar

Dette er det opphavlege undersøkingsnotatet. Integrasjonen er no implementert; sjå [HBV-vassføring i Respons](nve_hbv.md) for gjeldande kontrakt og utrulling.

Undersøkt 2026-10-28 med lesekall til HydAPI, VEPS og offentleg seNorge-klientkode. Stasjon `12.215.0` heiter **Storeskar** i svara frå NVE. Ingen endringar er gjorde i synk, database eller produksjons-API.

## Konklusjon

«Vannføring – HBV» under modellfelt og 24 timars data i seNorge blir henta frå **VEPS API**, ikkje som ein vanleg serie i HydAPI. seNorge byggjer denne serien med datakjelde `hf_hbv`, element `6004` og døgnoppløysing `P1D`. Direkte GET- og POST-kall mot VEPS gav data for Storeskar utan API-nøkkel i desse testane.

Dette er skilt frå dei eksisterande NVE-flytane i proxyen:

| Data | Kjelde / identifikasjon |
| --- | --- |
| Observasjonsbasert vassføring | HydAPI, parameter `1001` |
| Persentilar | HydAPI `Percentiles` |
| Kulminerte flaumtersklar | Chartserver `qcm`, `qc5`, `qc50`, med serie-ID `<stasjon>.1001.<versjon>` |
| HBV-modellert vassføring i dette seNorge-plottet | VEPS, `dataSource=hf_hbv`, `elements=6004` (`sim_discharge`) |

## Kva som vart kontrollert

- HydAPI `/Parameters` gav 47 parameterar, utan `6004` eller HBV.
- HydAPI `/Series?StationId=12.215.0` gav tre seriar: vasstand (`1000`), vassføring (`1001`) og lufttemperatur (`17`), alle med versjon 1. Eit ekstra kall med `Parameter=6004` gav HTTP `404`.
- Dette stadfestar at HBV ikkje var eksponert gjennom desse HydAPI-oppslaga på undersøkingsdatoen; det er ikkje ein garanti for framtidig API-innhald.
- NVE dokumenterer òg historiske HBV-seriar via Chartserver med parameter/versjon `6004.3`. Eit avgrensa kall til `12.215.0.6004.3` med `da=37`, `rt=1`, `mth=mean` for 20.–28. oktober gav «Ingen data». Den generelle Chartserver-oppskrifta gav dermed ikkje den aktuelle serien i dette forsøket.
- seNorge sin grafmodul brukar `VepsApiSerie` med `6004` og vald modelltype. Grafbiblioteket sender lesekall til `https://vepsapi.nve.no/timeseriesdata/read`.
- VEPS med `hf_hbv`, `6004`, `P1D` og `PT6H` gav HTTP `200` og 11 punkt for 20.–30. oktober. Responsen oppgav `method_name=mean`, `unit=m^3/s` og tidsstempel kl. 06:00 UTC.
- Same VEPS-førespurnad med `odm_hbv` gav HTTP `200`, men tom `elements`-liste. Dette gjeld berre den testa stasjonen, perioden og parameterkombinasjonen.

## Reproduserbart døme

Dette Python-dømet les døgnverdiar. Det krev berre `requests`, som prosjektet allereie brukar:

```python
import requests

response = requests.get(
    "https://vepsapi.nve.no/timeseriesdata/read",
    params={
        "locations": "12.215.0",
        "referenceTime": "2026-10-20/2026-10-31",
        "dataSource": "hf_hbv",
        "elements": "6004",
        "zone": "false",
        "ensemble": "false",
        "timeOffsets": "PT6H",
        "timeResolutions": "P1D",
        "generatedTime": "1900-01-01/2100-01-01",
    },
    timeout=30,
)
response.raise_for_status()
payload = response.json()
for location in payload.get("locations", []):
    for element in location.get("elements", []):
        for point in element.get("data", []):
            print(point["reference_time"], point["values"], element["unit"])
```

Eit faktisk datapunkt frå testresponsen var:

```json
{"source":"hf_hbv","reference_time":"2026-10-27T06:00:00Z","values":[3.003]}
```

Dette er ein modellverdi i m³/s, ikkje ei måling frå stasjonen. Døgnoppløysing er ikkje det same som maksimal momentanvassføring i døgnet. `PT6H` er tidsforskyvinga som seNorge nyttar i dette oppsettet; responsen si `reference_time` bør bevarast. Kva tidsstempelet representerer som start/slutt på berekningsperioden, må avklarast før ein koplar dette til proxyen sitt lokale kl. 06–06-døgn.

## Modellvariant og køyringstid

NVE dokumenterer `hf_hbv` som den eldre HBV-modellen og `odm_hbv` som HBV i ODM. seNorge skil dei som HBV og ODM. Dei skal derfor ikkje bytast om berre fordi begge har HBV i namnet.

`generatedTime` filtrerer modellkøyringar etter når dei vart genererte; den nyaste innanfor intervallet blir vald. Det vide intervallet i dømet følgjer seNorge og vel ikkje ei fast historisk køyring. Same førespurnad kan derfor gi andre verdiar seinare. Responsen sitt `created_at` er tidspunktet for API-svaret og må ikkje utan vidare brukast som tidspunkt for modellkøyringa.

Modellaget i seNorge hadde òg metadata for `hf_hbv`, `odm_hbv` og andre modellar på Storeskar, men somme oppdateringstidspunkt var gamle. At metadata viser ein modell, garanterer ikkje at det finst verdiar for eit bestemt tidsrom; kontroller sjølve tidsserieresponsen.

## Konsekvensar for ei eventuell integrering

Modellserien bør få eit eige logisk felt, til dømes `discharge_hbv`, med modelltype, oppløysing og tidsgrunnlag. Han bør ikkje skrivast inn som observert `discharge` eller oppdatere stasjonen sin observasjonsferskheit.

VEPS kan returnere framtidige modellpunkt. Testen gav mellom anna punkt for 29. og 30. oktober, etter undersøkingsdatoen. Proxyen sine noverande kvalitetskontrollar avviser framtidige observasjonar; ei modell-/prognoseintegrering treng derfor eiga handsaming.

Proxyen hentar no kulminerte flaumtersklar (`QCm`, `QC5`, `QC50`). seNorge-oppsettet brukar `qm;q5;q50` for døgnplott og `culqm;culq5;culq50` for tretimarsplott. Ein må avklare passande terskelgrunnlag før døgnmiddel frå HBV blir klassifisert med eksisterande tersklar.

## Kjelder

- [HydAPI-dokumentasjon](https://hydapi.nve.no/UserDocumentation/)
- [NVE: historiske vassføringsdata og HBV via Chartserver](https://www.nve.no/vann-og-vassdrag/hydrologiske-data/historiske-data/historiske-vannfoeringsdata-til-produksjonsplanlegging/)
- [NVE sitt Chartserver-døme i Python](https://github.com/NVE/API/blob/main/hydrological_data/hydrological_data_chartserver.py)
- [VEPS: tilgjengelege tidsseriar](https://vepsapi.nve.no/TimeSeries.md)
- [VEPS: datakjelder](https://vepsapi.nve.no/DataSources.md)
- [VEPS: element og statistikk](https://vepsapi.nve.no/Elements.md)
- [VEPS: GET-format](https://vepsapi.nve.no/GetTimeSeriesDataRead.md)
- [VEPS: val av modellkøyring](https://vepsapi.nve.no/GeneratedTime.md)
- [seNorge](https://senorge.no/): offentleg klientkode undersøkt i `assets/MapView-D3gXvTFu.js`, `assets/index-f5txkeB-.js` og grafbiblioteket `https://cdn.nve.no/nvechart/v2.0.7/nvechart.bundle.js`. Filnamna kan endre seg ved ny utrulling.
- [seNorge sitt offentlege modellag](https://gis3.nve.no/map/rest/services/Mapservices/xgeoHydModell/MapServer/0)
