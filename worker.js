/* Runs Settle in a Web Worker: Pyodide (CPython on WebAssembly), the repository's own Python
 * packages, and a real-time loop that drives the simulated world. The page only renders. */

let pyodide = null;
let demo = null;
let speed = 10;          // simulated seconds per real second
let paused = false;
let view = "all";
let watching = null;     // payment id open in the drawer
let feedSince = 0;
let last = 0;
let busy = 0;            // ms spent inside Python since the last snapshot
let lastSnap = 0;
let lastDetail = 0;
let lag = false;

const post = (msg) => self.postMessage(msg);

async function start({ indexURL, sources }) {
  post({ type: "progress", stage: "runtime", text: "Downloading Python (WebAssembly)" });
  importScripts(indexURL + "pyodide.js");
  pyodide = await loadPyodide({ indexURL, stdout: () => {}, stderr: (s) => console.warn(s) });

  post({ type: "progress", stage: "source", text: "Loading Settle's source code" });
  const zip = await (await fetch(sources)).arrayBuffer();
  pyodide.unpackArchive(zip, "zip", { extractDir: "/settle" });

  post({ type: "progress", stage: "engine", text: "Starting the engine" });
  pyodide.runPython(`
import sys
sys.path[:0] = ["/settle", "/settle/demo"]
import stubs
stubs.install()
from sim.live import Demo
demo = Demo(seed=7)
`);
  demo = pyodide.globals.get("demo");
  const catalogue = JSON.parse(pyodide.runPython("Demo.catalogue()"));
  catalogue.python = pyodide.runPython("import sys; sys.version.split()[0]");
  post({ type: "ready", catalogue });
  last = performance.now();
  snapshot();
  setInterval(tick, 50);
}

function timed(fn) {
  const t = performance.now();
  try {
    return fn();
  } finally {
    busy += performance.now() - t;
  }
}

function tick() {
  const now = performance.now();
  const dt = Math.min(now - last, 250) / 1000;
  last = now;
  if (!paused && demo) {
    try {
      lag = timed(() => demo.advance(dt * speed));
    } catch (e) {
      post({ type: "error", message: String(e) });
      paused = true;
    }
  }
  if (now - lastSnap >= 250) snapshot();
  if (watching && now - lastDetail >= 700) detail(watching);
}

function snapshot() {
  const now = performance.now();
  const elapsed = now - lastSnap || 1;
  const data = JSON.parse(timed(() => demo.snapshot(feedSince, view)));
  if (data.feed.length) feedSince = data.feed[data.feed.length - 1].id;
  post({ type: "snapshot", data, perf: { load: Math.min(1, busy / elapsed), lag, speed, paused } });
  busy = 0;
  lastSnap = now;
}

function detail(pid) {
  lastDetail = performance.now();
  post({ type: "detail", pid, data: JSON.parse(timed(() => demo.detail(pid))) });
}

function reset(seed) {
  demo.destroy();
  pyodide.runPython(`demo = Demo(seed=${seed | 0})`);
  demo = pyodide.globals.get("demo");
  feedSince = 0;
  watching = null;
  post({ type: "reset" });
  snapshot();
}

self.onmessage = async ({ data: m }) => {
  try {
    switch (m.type) {
      case "init": await start(m); break;
      case "speed": speed = m.value; snapshot(); break;
      case "pause": paused = m.value; last = performance.now(); snapshot(); break;
      case "view": view = m.value; snapshot(); break;
      case "control": timed(() => demo.control(JSON.stringify(m.payload))); snapshot(); break;
      case "watch": watching = m.pid; if (m.pid) detail(m.pid); break;
      case "replay":
        post({ type: "replay", pid: m.pid, data: JSON.parse(timed(() => demo.replay(m.pid))) });
        break;
      case "reset": reset(m.seed); break;
    }
  } catch (e) {
    post({ type: "error", message: String(e && e.message || e) });
  }
};
