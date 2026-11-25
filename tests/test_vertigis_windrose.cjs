const assert = require('node:assert/strict');
const fs = require('node:fs');
const path = require('node:path');
const vm = require('node:vm');
const { test } = require('node:test');

const END = Date.parse('2026-11-24T12:00:00Z');
const HOUR = 3600000;
const source = fs.readFileSync(path.join(__dirname, '../docs/vertigis/weather-chart.expression.txt'), 'utf8');
// Expose the expression's private functions only in this test execution.
const instrumented = source.slice(1).replace('title: firstStationOptions.title,',
  '__test: { calculateWindRoseData, makeWindRoseSpeedClasses, findWindSpeedClass, getWindRoseEndTime, getWindRoseRadialMax, buildWindRoseOptions, showWindRoseOverlay, closeWindRoseOverlay, makeStationModeButtons, makeParameterModeButtons }, title: firstStationOptions.title,');
const input = { station: { source_id: 'test' }, series: {
  wind_speed: { data: [{ timestamp: END, value: 4 }] },
  wind_from_direction: { data: [{ timestamp: END, value: 0 }] }
}};

function hooks(raw = input, globals = {}) {
  const options = new vm.Script(instrumented).runInNewContext({
    $webRequestAlle: { json: raw },
    Date: class extends Date { static now() { return END; } }, ...globals
  });
  assert.ok(options.__test, 'Expected instrumentation point in expression');
  return options.__test;
}

function api(samples) {
  return { series: {
    wind_speed: { data: samples.map(([speed, direction, timestamp = END]) => ({ timestamp, value: speed })) },
    wind_from_direction: { data: samples.map(([speed, direction, timestamp = END]) => ({ timestamp, value: direction })) }
  }};
}

test('all Beaufort boundaries are gap-free above the 3.4 m/s threshold', () => {
  const h = hooks();
  const classes = h.makeWindRoseSpeedClasses();
  const thresholds = [3.4, 5.5, 8, 10.8, 13.9, 17.2, 20.8, 24.5, 28.5, 32.7];
  thresholds.forEach((value, index) => {
    assert.equal(h.findWindSpeedClass(value, classes), index);
    assert.equal(classes[index].beaufort, index + 3);
    assert.equal(h.findWindSpeedClass(value - 0.0001, classes), index - 1);
  });
  assert.equal(h.findWindSpeedClass(100, classes), 9);
});

test('percentages include calm readings without direction and report unmatched stronger wind', () => {
  const h = hooks();
  const result = h.calculateWindRoseData(api([
    [0, null, END], [2.999, null, END - HOUR],
    [3.4, 360, END - 2 * HOUR], [5.5, 90, END - 3 * HOUR],
    [8, null, END - 4 * HOUR], [12, -999, END - 5 * HOUR],
    [-1, 0, END - 6 * HOUR], [NaN, 0, END - 7 * HOUR],
    [Infinity, 0, END - 8 * HOUR], [null, 0, END - 9 * HOUR]
  ]));
  assert.equal(result.validCount, 6);
  assert.equal(result.calmCount, 2);
  assert.equal(result.directionalCount, 2);
  assert.equal(result.missingDirectionCount, 2);
  assert.ok(Math.abs(result.speedClasses[0].values[0] - 100 / 6) < 1e-10);
  assert.ok(Math.abs(result.speedClasses[1].values[2] - 100 / 6) < 1e-10);
  const petals = result.speedClasses.reduce((sum, c) => sum + c.values.reduce((s, v) => s + v, 0), 0);
  assert.ok(Math.abs(petals + result.calmPercent + result.missingDirectionCount / result.validCount * 100 - 100) < 1e-10);
});

test('time windows use (start, end], reject future data and deduplicate timestamps', () => {
  const h = hooks();
  const data = api([
    [4, 0, END - 24 * HOUR], [4, 0, END - 6 * HOUR],
    [2, null, END - 6 * HOUR + 1], [4, 0, END],
    [9, 0, END + 1], [5, 90, END]
  ]);
  const six = h.calculateWindRoseData(data, { from: END - 6 * HOUR, to: END });
  const day = h.calculateWindRoseData(data, { from: END - 24 * HOUR, to: END });
  assert.equal(six.validCount, 2);
  assert.equal(six.calmPercent, 50);
  assert.equal(six.speedClasses[0].values[2], 50);
  assert.equal(day.validCount, 3);
  assert.equal(h.calculateWindRoseData(data).validCount, 4);
});

test('all calm, all windy, and empty windows have distinct valid results', () => {
  const h = hooks();
  const calm = h.calculateWindRoseData(api([[2.9, null]]));
  const windy = h.calculateWindRoseData(api([[5, 0]]));
  const empty = h.calculateWindRoseData(api([]));
  assert.equal(calm.calmPercent, 100);
  assert.equal(calm.validCount, 1);
  assert.equal(windy.calmPercent, 0);
  assert.equal(windy.speedClasses[0].values[0], 100);
  assert.equal(empty.calmPercent, null);
  assert.equal(h.getWindRoseRadialMax([calm, windy, empty]), 100);
  assert.equal(h.getWindRoseRadialMax([empty]), 10);
  const mixed = h.calculateWindRoseData(api([[1, null, END], [5, 0, END - HOUR]]));
  assert.equal(h.getWindRoseRadialMax([mixed]), 50, 'Scale excludes the removed low-wind readings');
});

test('whole-period range and open hurricane class survive JSON serialization', () => {
  const h = hooks();
  const range = JSON.parse(JSON.stringify({ from: null, to: END }));
  const result = h.calculateWindRoseData(api([[3.399, null, END - 30 * HOUR], [3.4, 0, END], [40, 90, END - HOUR]]), range);
  assert.equal(result.validCount, 3);
  assert.equal(result.calmCount, 1);
  assert.equal(result.directionalCount, 2);
  const classes = JSON.parse(JSON.stringify(h.makeWindRoseSpeedClasses()));
  assert.equal(h.findWindSpeedClass(40, classes), 9);
});

test('all stations share the response end time, capped at now', () => {
  const earlier = END - 48 * HOUR;
  assert.equal(hooks({ stations: [input], to: new Date(earlier).toISOString() }).getWindRoseEndTime([input]), earlier);
  assert.equal(hooks({ stations: [input], to: new Date(END + HOUR).toISOString() }).getWindRoseEndTime([input]), END);
  assert.equal(hooks().getWindRoseEndTime([api([[3, 0, earlier]]), input]), END);
});

test('actual observation bounds include low wind and ignore invalid or future measurements', () => {
  const h = hooks();
  const result = h.calculateWindRoseData(api([
    [NaN, 0, END - 50 * HOUR], [2, null, END - 30 * HOUR],
    [5, 0, END - 2 * HOUR], [6, 90, END - HOUR], [9, 0, END + HOUR]
  ]));
  assert.equal(result.firstObservationTime, END - 30 * HOUR);
  assert.equal(result.lastObservationTime, END - HOUR);
  const empty = h.calculateWindRoseData(api([]));
  assert.equal(empty.firstObservationTime, null);
  assert.equal(empty.lastObservationTime, null);
});

test('roses start at zero without a center marker and report the excluded share', () => {
  const h = hooks();
  const result = h.calculateWindRoseData(api([[2, null, END], [5, 0, END - HOUR]]));
  const options = h.buildWindRoseOptions(input, result, h.getWindRoseRadialMax([result]));
  assert.equal(options.series.length, 10);
  assert.ok(options.series.every(series => series.data.length === 8));
  assert.ok(options.subtitle.text.includes('Fjerna (<3,4 m/s): 50.0 %'));
  assert.equal(options.yAxis.min, 0);
  assert.equal(options.yAxis.max, 50);
  assert.equal(options.chart.events, undefined);
  assert.equal(options.tooltip.shared, false);
});

test('eight Norwegian directions include the north wrap and exact sector boundaries', () => {
  const h = hooks();
  const directions = [0, 22.499, 22.5, 45, 90, 135, 180, 225, 270, 315, 337.5, 360];
  const result = h.calculateWindRoseData(api(directions.map((direction, index) => [4, direction, END - index * HOUR])));
  assert.equal(Array.from(result.directions).join(','), 'N,NØ,Ø,SØ,S,SV,V,NV');
  const counts = result.speedClasses[0].values.map(percent => Math.round(percent * directions.length / 100));
  assert.equal(Array.from(counts).join(','), '4,2,1,1,1,1,1,1');
});

test('period buttons rebuild all roses after laying out cards and clean up old charts', () => {
  function element() {
    return { children: [], style: {}, attributes: {},
      get firstChild() { return this.children[0]; },
      appendChild(child) { this.children.push(child); child.parentNode = this; },
      removeChild(child) { this.children.splice(this.children.indexOf(child), 1); child.parentNode = null; },
      addEventListener() {}, setAttribute(name, value) { this.attributes[name] = value; } };
  }
  const document = { createElement: element };
  const samples = [api([[2, null, END], [5, 0, END - 10 * HOUR]]),
    api([[5, 90, END - 7 * HOUR]])];
  const h = hooks({ to: new Date(END).toISOString(), stations: samples }, { document });
  let created = 0;
  let destroyed = 0;
  const chart = { renderTo: element(), constructor: function(container, options) {
    assert.equal(container.parentNode.children.length, 2, 'All cards exist before chart sizing');
    this.options = options;
    this.destroy = () => { destroyed++; };
    created++;
  }};
  h.showWindRoseOverlay(chart, samples);
  const overlay = chart.weatherWindRoseOverlay;
  const controls = overlay.children[0].children[1];
  const [all, day, six, close] = controls.children;
  assert.equal(created, 2);
  assert.equal(all.attributes['aria-pressed'], 'true');
  const periodText = overlay.children[1];
  assert.ok(periodText.textContent.includes('03:00'), periodText.textContent);
  assert.ok(periodText.textContent.includes('13:00'), periodText.textContent);
  assert.ok(!periodText.textContent.includes('Heile den innlasta'));
  day.onclick();
  assert.equal(day.attributes['aria-pressed'], 'true');
  assert.equal(destroyed, 2);
  six.onclick();
  assert.equal(destroyed, 4);
  assert.ok(periodText.textContent.includes('07:00'), periodText.textContent);
  assert.ok(chart.weatherWindRoseCharts[0].options.subtitle.text.startsWith('1 målingar'));
  assert.ok(chart.weatherWindRoseCharts[1].options.subtitle.text.startsWith('Ingen gyldige'));
  all.onclick();
  assert.equal(destroyed, 6);
  assert.ok(periodText.textContent.includes('03:00'), periodText.textContent);
  assert.ok(chart.weatherWindRoseCharts[0].options.subtitle.text.startsWith('2 målingar'));
  close.onclick();
  assert.equal(destroyed, 8);
  assert.equal(chart.renderTo.children.length, 0);
  assert.equal(chart.weatherWindRoseCharts.length, 0);
});


test('wind rose button follows valid wind data across both toolbar modes and dataset changes', () => {
  const h = hooks();
  const noWind = { station: { source_id: 'none' }, series: {} };
  const invalid = api([[null, 0, END], [-1, 0, END - HOUR], [NaN, 0, END - 2 * HOUR], [5, 0, END + HOUR]]);
  const zeroWind = { station: { source_id: 'zero' }, ...api([[0, null, END]]) };
  for (const makeButtons of [h.makeStationModeButtons, h.makeParameterModeButtons]) {
    const chart = { chartWidth: 1100 };
    const visible = makeButtons(chart, [noWind, input], 0).windrose;
    assert.equal(visible.text, 'Vindroser');
    // Simulate chart.update merging its previous buttons with the new options.
    const merged = { ...visible, ...makeButtons(chart, [noWind, invalid], 0).windrose };
    assert.equal(merged.text, '');
    assert.ok(merged.x < 0);
    assert.equal(merged.onclick(), false);
    assert.equal(makeButtons(chart, [zeroWind], 0).windrose.text, 'Vindroser');
    assert.equal(makeButtons(chart, [noWind, input], 0).windrose.text, 'Vindroser');
  }
});
