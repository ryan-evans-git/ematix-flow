// Pure helpers: map a query result ({columns, rows}) + an encoding into
// an Apache ECharts option. No ECharts import here — this stays cheap
// and unit-testable; the heavy library is lazy-loaded by EChart.svelte.

export const VIZ_TYPES = [
  { id: "table", label: "Table", icon: "▦" },
  { id: "number", label: "Big Number", icon: "#" },
  { id: "bar", label: "Bar", icon: "▍" },
  { id: "line", label: "Line", icon: "╱" },
  { id: "area", label: "Area", icon: "◣" },
  { id: "pie", label: "Pie", icon: "◕" },
  { id: "scatter", label: "Scatter", icon: "∴" },
];

const NUMERIC = new Set(["int", "double", "decimal"]);

export function isNumericType(type) {
  return NUMERIC.has(String(type || "").toLowerCase());
}

const idx = (columns, name) => columns.findIndex((c) => c.name === name);

// Suggest a sensible default encoding for a viz type given the columns:
// first non-numeric column as the dimension, first numeric as the metric.
export function defaultEncoding(vizType, columns) {
  const dims = columns.filter((c) => !isNumericType(c.type)).map((c) => c.name);
  const nums = columns.filter((c) => isNumericType(c.type)).map((c) => c.name);
  const dim = dims[0] ?? columns[0]?.name;
  const num = nums[0] ?? columns[1]?.name ?? columns[0]?.name;
  switch (vizType) {
    case "pie":
      return { name: dim, value: num };
    case "scatter":
      return { x: nums[0] ?? dim, y: [nums[1] ?? num] };
    case "number":
      return { value: num };
    default: // bar / line / area
      return { x: dim, y: num ? [num] : [] };
  }
}

// Big-number value: sum the metric across rows when numeric, else the
// first row's value.
export function bigNumberValue(columns, rows, enc) {
  const vi = idx(columns, enc.value);
  if (vi < 0 || !rows.length) return null;
  const numeric = rows.every((r) => r[vi] === null || typeof r[vi] === "number");
  if (numeric) {
    return rows.reduce((a, r) => a + (r[vi] || 0), 0);
  }
  return rows[0][vi];
}

export function buildEChartsOption(vizType, columns, rows, enc = {}) {
  if (!columns || !columns.length || !rows) return null;
  const base = {
    backgroundColor: "transparent",
    tooltip: {},
    grid: { left: 52, right: 24, top: 36, bottom: 44, containLabel: true },
  };

  switch (vizType) {
    case "line":
    case "bar":
    case "area": {
      const xi = idx(columns, enc.x);
      const ys = (enc.y || []).filter((y) => idx(columns, y) >= 0);
      if (xi < 0 || !ys.length) return null;
      const categories = rows.map((r) => r[xi]);
      const series = ys.map((y) => ({
        name: y,
        type: vizType === "area" ? "line" : vizType,
        areaStyle: vizType === "area" ? {} : undefined,
        smooth: vizType !== "bar",
        data: rows.map((r) => r[idx(columns, y)]),
      }));
      return {
        ...base,
        legend: { top: 6, data: ys },
        xAxis: { type: "category", data: categories },
        yAxis: { type: "value" },
        series,
      };
    }
    case "pie": {
      const ni = idx(columns, enc.name);
      const vi = idx(columns, enc.value);
      if (ni < 0 || vi < 0) return null;
      const data = rows.map((r) => ({ name: String(r[ni]), value: r[vi] }));
      return {
        ...base,
        tooltip: { trigger: "item" },
        legend: { top: 6 },
        series: [{ type: "pie", radius: ["38%", "66%"], data }],
      };
    }
    case "scatter": {
      const xi = idx(columns, enc.x);
      const yi = idx(columns, (enc.y || [])[0]);
      if (xi < 0 || yi < 0) return null;
      const data = rows.map((r) => [r[xi], r[yi]]);
      return {
        ...base,
        xAxis: { type: "value", name: enc.x },
        yAxis: { type: "value", name: (enc.y || [])[0] },
        series: [{ type: "scatter", symbolSize: 9, data }],
      };
    }
    default:
      return null; // table / number rendered outside ECharts
  }
}
