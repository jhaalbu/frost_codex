const assert = require('node:assert/strict');
const fs = require('node:fs');
const path = require('node:path');
const vm = require('node:vm');
const { test } = require('node:test');

const expressionPath = path.join(__dirname, '../docs/vertigis/weather-chart.expression.txt');
const time = Date.parse('2026-11-24T00:00:00Z');
const item = (values, unit = 'degC') => ({unit, data: values.map((value, i) => ({timestamp: time + i * 3600000, value}))});
const stations = [
  {station: {source_id: 'SN123', name: 'Both temperatures', masl: 900}, series: {
    air_temperature: item([-5, 0, 2]), surface_temperature: item([-12, 0, 1]),
    precipitation_1h: item([0, 1, 2], 'mm'), precipitation_accumulated: item([0, 1, 3], 'mm'),
    wind_speed: item([3, 4, 5], 'm/s'), wind_gust_10m: item([6, 7, 8], 'm/s'),
    wind_speed_of_gust: item([9, 10, 11], 'm/s'), wind_from_direction: item([0, 90, 180], 'deg'),
    snow_depth: item([20, 21, 22], 'cm'), groundwater_level: item([1, 2, 3], 'm'),
    snow_drift_low: item([1, 2, 3]), snow_drift_high: item([3, 4, 5]),
    snow_drift_low_cumsum: item([1, 3, 6]), snow_drift_high_cumsum: item([2, 4, 8]),
    wind_speed_fc_low: item([1, 2, 3]), wind_speed_fc_high: item([2, 3, 4])}},
  {station: {source_id: 'SNWR-test', name: 'Surface only'}, series: {surface_temperature: item([-20, -10])}},
  {station: {source_id: '12.215.0', name: 'Discharge'}, series: {discharge: {
    ...item([2, 3, 4], 'm3/s'), thresholds: {qm: 5, q5: 10, q50: 20},
    percentiles: [0, 1, 2].map(i => ({timestamp: time + i * 3600000, perc25: 1, perc75: 5}))}}}
];

function evaluate(file = expressionPath, input = {stations}) {
  let csv;
  const document = {createEvent: () => ({initCustomEvent() {}}),
    createElement: () => ({style: {}, click() {csv = decodeURIComponent(this.href.split(',').slice(1).join(','));}}),
    body: {appendChild() {}, removeChild() {}}};
  const source = fs.readFileSync(file, 'utf8').slice(1).replace('title: firstStationOptions.title,',
    '__test: { buildStationSpecificOptions, buildAllStationsSingleParameterOptions, makeStationModeButtons, makeParameterModeButtons }, title: firstStationOptions.title,');
  const options = new vm.Script(source).runInNewContext({$webRequestAlle: {json: input}, document,
    window: {document, dispatchEvent() {}}, alert(message) {throw new Error(message);}});
  return {options, getCsv: () => csv};
}

function chartFor(options) {
  const chart = {chartWidth: 1100, series: [], yAxis: [], exporting: options.exporting,
    container: {addEventListener() {}, removeEventListener() {}},
    redraw() {}, setTitle(title) {this.title = title;},
    update(update) {this.lastUpdate = update; if (update.exporting) this.exporting = update.exporting;},
    get(id) {return this.yAxis.find(a => a.options.id === id);},
    addAxis(options) {const axis = {options, remove() {chart.yAxis.splice(chart.yAxis.indexOf(axis), 1);}}; this.yAxis.push(axis);},
    addSeries(options) {const series = {...options, visible: true,
      data: options.data.map(p => Array.isArray(p) ? {x: p[0], y: p[1]} : p),
      remove() {chart.series.splice(chart.series.indexOf(series), 1);}}; this.series.push(series);}
  };
  options.yAxis.forEach(a => chart.addAxis(a)); options.series.forEach(s => chart.addSeries(s));
  return chart;
}

test('temperature grouping, zero values, gust fallback and discharge reference data', () => {
  const {options} = evaluate();
  const temperatures = options.series.filter(s => s.yAxis === 'air_temperature');
  assert.equal(temperatures.length, 2);
  assert.equal(temperatures[0].dashStyle, 'Solid');
  assert.equal(temperatures[1].dashStyle, 'ShortDash');
  assert.equal(temperatures[1].data[1][1], 0);
  assert.equal(options.series.filter(s => s.yAxis === 'wind').length, 2);
  const discharge = options.__test.buildStationSpecificOptions(stations[2], null);
  assert.equal(discharge.yAxis[0].plotLines.length, 3);
  assert.equal(discharge.series.length, 3);
  assert.ok(discharge.series.some(s => s.dashStyle === 'ShortDash'));
});

test('parameter switches, shared temperature scale, station return and CSV preserve both series', () => {
  const result = evaluate(); const chart = chartFor(result.options);
  chart.exporting.buttons.station_all.onclick.call(chart);
  chart.exporting.buttons.param_air_temperature.onclick.call(chart);
  assert.equal(chart.yAxis.length, 2);
  assert.equal(chart.series.length, 3);
  assert.equal(chart.yAxis[0].options.min, -20);
  assert.equal(chart.yAxis[1].options.min, -20);
  chart.exporting.buttons.lastNedCSV.onclick.call(chart);
  assert.ok(result.getCsv().includes('Overflatetemperatur'));
  assert.ok(result.getCsv().includes('Lufttemperatur'));
  assert.ok(result.getCsv().includes('-20'));
  chart.exporting.buttons.param_wind.onclick.call(chart);
  assert.equal(chart.yAxis.length, 2); // Speed plus direction subplot.
  chart.exporting.buttons.single_station_mode.onclick.call(chart);
  chart.exporting.buttons.station_1.onclick.call(chart);
  assert.equal(chart.series.length, 1);
  assert.ok(chart.series[0].name.includes('Surface only'));
});

test('input shapes and missing series remain supported', () => {
  for (const input of [stations, {stations}, {items: stations}, {data: stations}, {results: stations}]) {
    assert.equal(evaluate(expressionPath, input).options.series.length, evaluate().options.series.length);
  }
  assert.equal(evaluate(expressionPath, null).options.series.length, 0);
  const surfaceOnly = evaluate(expressionPath, stations[1]).options;
  assert.equal(surfaceOnly.yAxis.length, 1);
  assert.equal(surfaceOnly.series.length, 1);
});

// Optional before/after comparison for refactors; callbacks are invoked separately above.
if (process.env.VERTIGIS_BASELINE) {
  test('generated axes, data, styles and toolbar options equal the pre-refactor expression', () => {
    const before = evaluate(process.env.VERTIGIS_BASELINE).options.__test;
    const after = evaluate().options.__test;
    const clean = object => JSON.parse(JSON.stringify(object, (key, value) => typeof value === 'function' ? '[callback]' : value));
    for (const width of [420, 1100]) {
      const chart = {chartWidth: width};
      for (const station of stations) {
        assert.deepEqual(clean(after.buildStationSpecificOptions(station, chart)), clean(before.buildStationSpecificOptions(station, chart)));
      }
      for (const key of ['air_temperature', 'wind', 'discharge', 'snow_depth', 'precipitation_1h', 'snow_drift', 'missing']) {
        assert.deepEqual(clean(after.buildAllStationsSingleParameterOptions(stations, key, chart)), clean(before.buildAllStationsSingleParameterOptions(stations, key, chart)));
      }
      assert.deepEqual(clean(after.makeStationModeButtons(chart, stations, 1)), clean(before.makeStationModeButtons(chart, stations, 1)));
      assert.deepEqual(clean(after.makeParameterModeButtons(chart, stations, 'air_temperature')), clean(before.makeParameterModeButtons(chart, stations, 'air_temperature')));
    }
  });
}
