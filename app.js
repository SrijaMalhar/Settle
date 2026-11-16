/* The page: renders what the worker reports and sends it the controls. No state of its own
 * beyond what is on screen; the world lives in the worker. */
"use strict";

const $ = (s, el = document) => el.querySelector(s);
const $$ = (s, el = document) => [...el.querySelectorAll(s)];
const esc = (s) => String(s ?? "").replace(/[&<>"']/g, (c) => ({ "&": "&amp;", "<": "&lt;", ">": "&gt;", '"': "&quot;", "'": "&#39;" }[c]));
const COLORS = { alpha: "var(--alpha)", bravo: "var(--bravo)", charlie: "var(--charlie)" };
const RAW = { alpha: "#38bdf8", bravo: "#a78bfa", charlie: "#f472b6" };
const inr = (paise, dp = 2) => "₹" + (paise / 100).toLocaleString("en-IN", { minimumFractionDigits: dp, maximumFractionDigits: dp });
const clock = (t) => { t = Math.max(0, Math.floor(t)); const h = Math.floor(t / 3600), m = Math.floor(t / 60) % 60, s = t % 60; return `T+${String(h).padStart(2, "0")}:${String(m).padStart(2, "0")}:${String(s).padStart(2, "0")}`; };
const short = (t) => { t = Math.max(0, Math.floor(t)); const m = Math.floor(t / 60), s = t % 60; return m >= 60 ? `${Math.floor(m / 60)}h${String(m % 60).padStart(2, "0")}` : `${m}:${String(s).padStart(2, "0")}`; };
const age = (s) => s < 60 ? `${s}s` : s < 3600 ? `${Math.floor(s / 60)}m ${s % 60}s` : `${Math.floor(s / 3600)}h ${Math.floor(s / 60) % 60}m`;
const pct = (x) => `${Math.round(x * 100)}%`;

const ui = { catalogue: null, snap: null, view: "all", open: null, detail: null, replays: {}, raw: new Set(), feedIds: new Set(), paused: false, lastInv: {} };
const worker = new Worker("worker.js");
const send = (m) => worker.postMessage(m);

/* ------------------------------------------------------------------ loading */

const STAGES = ["runtime", "source", "engine"];
function progress(stage) {
  const i = STAGES.indexOf(stage);
  $$("#steps li").forEach((li, j) => li.className = j < i ? "done" : j === i ? "doing" : "");
  $("#progress").style.width = `${[12, 70, 88][i] || 5}%`;
}

function ready(catalogue) {
  ui.catalogue = catalogue;
  $$("#steps li").forEach((li) => li.className = "done");
  $("#progress").style.width = "100%";
  $("#runtime").textContent = `Python ${catalogue.python} · WebAssembly`;
  const sel = $("#bug");
  for (const [k, b] of Object.entries(catalogue.bugs)) sel.insertAdjacentHTML("beforeend", `<option value="${k}">${esc(b.title)}</option>`);
  $("#psps").innerHTML = Object.keys(catalogue.psps).map(pspCard).join("");
  $("#legend").innerHTML = Object.keys(catalogue.psps).map((n) => `<span><i style="background:${RAW[n]}"></i>${n}</span>`).join("")
    + `<span><i class="line" style="background:var(--warn)"></i>attempts in doubt</span><span><i class="line" style="background:var(--text);opacity:.5"></i>status checks</span>`;
  setTimeout(() => { $("#loading").hidden = true; $("#app").hidden = false; resizeChart(); }, 250);
}

/* ------------------------------------------------------------------ top level render */

function render(snap, perf) {
  ui.snap = snap;
  $("#clock").textContent = clock(snap.t);
  renderKpis(snap);
  renderPsps(snap);
  renderInvariants(snap);
  renderSwitches(snap);
  renderChart(snap.series);
  renderPayments(snap);
  renderFeed(snap.feed);
  const r = snap.recovery;
  $("#recovery").textContent = r ? `Last crash at ${clock(r.t)}: ${r.open} open payments rebuilt from the event log in ${r.ms} ms.` : "";
  const events = (snap.counts.events || 0).toLocaleString("en-IN");
  $("#perf").textContent = `Engine busy ${pct(perf.load)} of the worker's time at ${perf.speed}× · ${events} events stored`
    + (perf.lag ? " · running slower than requested on this device" : "") + (perf.paused ? " · paused" : "");
  $(".pill.live").classList.toggle("paused", perf.paused);
}

function kpi(k, v, s, cls = "", extra = "") {
  return `<div class="kpi ${extra}"><div class="k">${k}</div><div class="v ${cls}">${v}</div><div class="s">${s}</div></div>`;
}

function renderKpis(s) {
  const c = s.counts, st = s.status, m = s.money;
  const orders = c.orders || 0, paid = st.PAID || 0;
  const refunded = (st.REFUNDED || 0) + (st.REFUNDING || 0);
  const flight = m.net - m.owed;
  $("#kpis").innerHTML = [
    kpi("Orders", orders.toLocaleString("en-IN"), `${s.rate.toFixed(1)} arriving per second`),
    kpi("Paid", paid.toLocaleString("en-IN"), orders ? `${pct(paid / orders)} of orders` : "—", "ok"),
    kpi("In doubt", s.in_doubt, `${c.resolved_by_check || 0} settled by checks`, s.in_doubt ? "warn" : ""),
    kpi("Refunds", refunded, "late money returned"),
    kpi("Ops queue", st.NEEDS_OPS || 0, "waiting for a person", st.NEEDS_OPS ? "warn" : ""),
    kpi("Crashes survived", s.crashes, `${(c.recovered || 0).toLocaleString("en-IN")} rebuilt from events`),
    kpi("Money held by the PSPs", inr(m.net, 0), `paid orders ${inr(m.owed, 0)} · in flight ${inr(flight, 0)}`, "", "money"),
  ].join("");
}

/* ------------------------------------------------------------------ PSPs */

function pspCard(name) {
  return `<div class="psp" id="psp-${name}">
    <div class="top"><div><div class="who"><i style="background:${RAW[name]}"></i>${name}</div><div class="about">${esc(ui.catalogue.psps[name])}</div></div>
      <span class="breaker closed">CLOSED</span></div>
    <div class="stats">
      <div class="stat"><div class="l">Traffic</div><div class="n share">0%</div><div class="meter"><b style="background:${RAW[name]};width:0"></b></div></div>
      <div class="stat"><div class="l">Router score</div><div class="n score">—</div><div class="meter"><b class="sb" style="background:var(--muted);width:0"></b></div></div>
    </div>
    <div class="minute"><span class="l">last minute</span><span class="mi"></span></div>
    <div class="plan">status checks at <span class="checks-at">90 · 270 · 540</span> s</div>
    <div class="faults">
      <button class="fault" data-psp="${name}" data-fault="down" title="Refuse every connection">Down</button>
      <button class="fault" data-psp="${name}" data-fault="slow" title="Receive the request but reply after the 2 s timeout">Slow</button>
      <button class="fault" data-psp="${name}" data-fault="dropping" title="Never send callbacks">No callbacks</button>
    </div></div>`;
}

function renderPsps(s) {
  for (const [name, p] of Object.entries(s.psps)) {
    const el = $(`#psp-${name}`);
    if (!el) continue;
    const b = $(".breaker", el);
    b.className = `breaker ${p.breaker}`;
    b.textContent = p.breaker.replace("_", " ").toUpperCase();
    $(".share", el).textContent = pct(p.share);
    $(".meter b", el).style.width = pct(p.share);
    $(".score", el).innerHTML = p.score == null ? `—<span class="muted small"> no data</span>` : `${p.score.toFixed(2)}<span class="muted small"> · ${p.samples}</span>`;
    $(".sb", el).style.width = p.score == null ? "0" : pct(p.score);
    $(".checks-at", el).textContent = p.plan.join(" · ");
    const m = p.minute, failed = (m.failed || 0) + (m.failed_psp || 0);
    $(".mi", el).innerHTML = `<b>${p.sent_recent}</b> sent · answers: <b class="g">${m.clean || 0}</b> clean, <b class="y">${m.pending_success || 0}</b> after doubt, `
      + `<b class="${m.failed_psp ? "r" : ""}">${failed}</b> failed${m.failed_psp ? ` (${m.failed_psp} the PSP's fault)` : ""}`;
    for (const f of ["down", "slow", "dropping"]) $(`.fault[data-fault="${f}"]`, el).classList.toggle("on", p[f]);
  }
}

/* ------------------------------------------------------------------ invariants */

function renderInvariants(s) {
  const ul = $("#invariants");
  let html = "";
  for (const [id, inv] of Object.entries(s.invariants)) {
    const n = inv.violations, was = ui.lastInv[id] || 0;
    html += `<li class="${n ? "bad" : ""} ${n > was ? "flash" : ""}" data-inv="${id}"><span class="id">${id}</span><span>${esc(inv.name)}</span>`
      + `<span class="state ${n ? "" : "ok"}">${n ? `${n} violation${n > 1 ? "s" : ""}` : "holding"}</span></li>`;
    ui.lastInv[id] = n;
  }
  ul.innerHTML = html;
  const total = Object.values(s.invariants).reduce((a, i) => a + i.violations, 0);
  $("#inv-hint").textContent = total ? "a planted bug broke a guarantee" : "checked on every stored event";
  let list = $("#violations");
  if (!list) { ul.insertAdjacentHTML("afterend", `<ul class="violations" id="violations"></ul>`); list = $("#violations"); }
  list.innerHTML = s.violations.slice(0, 3).map((v) => `<li data-pid="${esc(v.pid || "")}"><b>${v.invariant}</b> ${clock(v.t)} · ${esc(v.detail)}</li>`).join("");
}

/* ------------------------------------------------------------------ switches */

function renderSwitches(s) {
  for (const el of $$("[data-switch]")) el.checked = !!s.switches[el.dataset.switch];
  const sel = $("#bug");
  if (document.activeElement !== sel) sel.value = s.switches.bug || "";
  sel.classList.toggle("on", !!s.switches.bug);
  const bug = s.switches.bug && ui.catalogue.bugs[s.switches.bug];
  $("#bug-what").textContent = bug ? bug.what : "Turn one on: the invariants on the left should catch it within a few simulated minutes.";
  if (document.activeElement !== $("#rate")) { $("#rate").value = s.rate; }
  $("#rate-out").textContent = `${Number(s.rate).toFixed(1)}/s`;
}

/* ------------------------------------------------------------------ chart */

const canvas = $("#chart");
function resizeChart() {
  const r = canvas.getBoundingClientRect(), d = window.devicePixelRatio || 1;
  canvas.width = Math.max(1, r.width * d);
  canvas.height = Math.max(1, r.height * d);
  if (ui.snap) renderChart(ui.snap.series);
}
new ResizeObserver(resizeChart).observe(canvas);

function renderChart(series) {
  const ctx = canvas.getContext("2d"), d = window.devicePixelRatio || 1;
  const W = canvas.width / d, H = canvas.height / d;
  ctx.setTransform(d, 0, 0, d, 0, 0);
  ctx.clearRect(0, 0, W, H);
  if (!series.length) return;
  const names = Object.keys(RAW), N = 90, padL = 30, padR = 34, padB = 20, padT = 8;
  const pw = W - padL - padR, ph = H - padT - padB, bw = pw / N;
  const totals = series.map((b) => names.reduce((a, n) => a + (b.sent[n] || 0), 0));
  const maxSent = Math.max(4, ...totals), maxDoubt = Math.max(4, ...series.map((b) => b.unknown)), maxChecks = Math.max(4, ...series.map((b) => b.checks));
  const niceMax = (m) => { const p = Math.pow(10, Math.floor(Math.log10(m))); return Math.ceil(m / p) * p; };
  const ySent = niceMax(maxSent), yDoubt = niceMax(Math.max(maxDoubt, maxChecks));
  ctx.font = "10.5px ui-monospace, Menlo, monospace";
  ctx.strokeStyle = "#1d2631"; ctx.fillStyle = "#5c6978"; ctx.lineWidth = 1;
  for (let i = 0; i <= 4; i++) {
    const y = padT + ph - (ph * i) / 4;
    ctx.beginPath(); ctx.moveTo(padL, y + 0.5); ctx.lineTo(W - padR, y + 0.5); ctx.stroke();
    ctx.textAlign = "right"; ctx.fillText(String(Math.round((ySent * i) / 4)), padL - 6, y + 3.5);
    ctx.textAlign = "left"; ctx.fillStyle = "#a3874a"; ctx.fillText(String(Math.round((yDoubt * i) / 4)), W - padR + 6, y + 3.5); ctx.fillStyle = "#5c6978";
  }
  const x0 = padL + (N - series.length) * bw;
  series.forEach((b, i) => {
    let y = padT + ph;
    for (const n of names) {
      const h = ((b.sent[n] || 0) / ySent) * ph;
      if (h > 0) { ctx.fillStyle = RAW[n]; ctx.globalAlpha = 0.85; ctx.fillRect(x0 + i * bw + 1, y - h, Math.max(1, bw - 2), h); y -= h; }
    }
  });
  ctx.globalAlpha = 1;
  const line = (key, color, alpha) => {
    ctx.beginPath(); ctx.strokeStyle = color; ctx.globalAlpha = alpha; ctx.lineWidth = 1.8; ctx.lineJoin = "round";
    series.forEach((b, i) => { const x = x0 + i * bw + bw / 2, y = padT + ph - (b[key] / yDoubt) * ph; i ? ctx.lineTo(x, y) : ctx.moveTo(x, y); });
    ctx.stroke(); ctx.globalAlpha = 1;
  };
  line("checks", "#e6edf3", 0.45);
  line("unknown", "#fbbf24", 1);
  ctx.fillStyle = "#5c6978"; ctx.textAlign = "center";
  for (const back of [15, 10, 5, 0]) {
    const x = padL + pw - (back * 6) * bw;
    ctx.fillText(back ? `−${back} min` : "now", x - (back ? 0 : 8), H - 5);
  }
}

/* ------------------------------------------------------------------ payments */

function renderPayments(s) {
  for (const b of $$("#views button")) {
    const n = s.views[b.dataset.view] || 0;
    const span = $("span", b);
    span.textContent = n.toLocaleString("en-IN");
    span.classList.toggle("some", n > 0);
  }
  const rows = s.payments.map((p) => `<tr data-pid="${p.pid}" class="${p.flagged ? "flagged" : ""}">
      <td class="order">${esc(p.order)}</td><td class="num">${inr(p.amount)}</td>
      <td><span class="att">${p.attempts.map((a) => `<span class="${a.status}${a.doubt && (a.status === "pending" || a.status === "sending") ? " doubt" : ""}" title="${a.psp}: ${a.status}${a.doubt ? ", went into doubt" : ""}">${a.psp[0].toUpperCase()}</span>`).join("")}</span></td>
      <td><span class="status st-${p.status}">${p.status.replace("_", " ")}</span></td><td class="num muted">${age(p.age)}</td></tr>`).join("");
  $("#payments").innerHTML = rows || `<tr class="empty"><td colspan="5">${{ all: "No payments yet", doubt: "Nothing in doubt: every live attempt has a known outcome", refunds: "No refunds yet", ops: "The ops queue is empty", flagged: "No invariant violations" }[ui.view]}</td></tr>`;
}

/* ------------------------------------------------------------------ feed */

function renderFeed(items) {
  if (!items.length) return;
  const ol = $("#feed");
  const html = items.filter((f) => !ui.feedIds.has(f.id)).reverse().map((f) => {
    ui.feedIds.add(f.id);
    return `<li class="${f.level} new" ${f.pid ? `data-pid="${esc(f.pid)}"` : ""}><i></i><time>${short(f.t)}</time><span>${esc(f.text)}</span></li>`;
  }).join("");
  ol.insertAdjacentHTML("afterbegin", html);
  while (ol.children.length > 90) ol.lastElementChild.remove();
  setTimeout(() => $$("li.new", ol).forEach((li) => li.classList.remove("new")), 400);
}

/* ------------------------------------------------------------------ drawer */

function openPayment(pid) {
  if (!pid) return;
  ui.open = pid;
  ui.raw.clear();
  $("#drawer-body").innerHTML = `<p class="muted">Loading ${esc(pid)}…</p>`;
  $("#drawer").scrollTop = 0;
  $("#drawer").classList.add("on");
  $("#drawer").setAttribute("aria-hidden", "false");
  $("#scrim").classList.add("on");
  send({ type: "watch", pid });
}

function closePayment() {
  ui.open = null;
  $("#drawer").classList.remove("on");
  $("#drawer").setAttribute("aria-hidden", "true");
  $("#scrim").classList.remove("on");
  send({ type: "watch", pid: null });
}

const NODES = { AWAITING: [70, 125], PAID: [215, 40], EXPIRED: [215, 210], CLOSED: [365, 40], NEEDS_OPS: [365, 125], REFUNDING: [365, 210], REFUNDED: [515, 210] };
const EDGES = [["AWAITING", "PAID"], ["AWAITING", "EXPIRED"], ["EXPIRED", "CLOSED"], ["EXPIRED", "NEEDS_OPS"], ["EXPIRED", "REFUNDING"],
  ["NEEDS_OPS", "CLOSED"], ["NEEDS_OPS", "REFUNDING", -11], ["REFUNDING", "NEEDS_OPS", 11], ["REFUNDING", "REFUNDED"]];

function machine(path) {
  const seen = new Set(path), now = path[path.length - 1];
  const walked = new Set(path.slice(1).map((s, i) => `${path[i]}>${s}`));
  const edges = EDGES.map((e) => [...e]);
  for (const key of walked) { const [a, b] = key.split(">"); if (!edges.some((e) => e[0] === a && e[1] === b)) edges.push([a, b, 0, true]); }
  const hw = 52, hh = 14;
  const clip = (x1, y1, x2, y2) => { const dx = x2 - x1, dy = y2 - y1; const t = Math.min(hw / Math.abs(dx || 1e-9), hh / Math.abs(dy || 1e-9)); return [x1 + dx * t, y1 + dy * t]; };
  let svg = `<svg viewBox="0 0 580 250" role="img" aria-label="Payment state machine, path ${path.join(" to ")}">`;
  for (const [a, b, off = 0, extra = false] of edges) {
    const [ax, ay] = NODES[a], [bx, by] = NODES[b];
    const [sx, sy] = clip(ax + off, ay, bx + off, by), [ex, ey] = clip(bx + off, by, ax + off, ay);
    const on = walked.has(`${a}>${b}`), ang = Math.atan2(ey - sy, ex - sx), L = 7;
    const p1 = [ex - L * Math.cos(ang - 0.4), ey - L * Math.sin(ang - 0.4)], p2 = [ex - L * Math.cos(ang + 0.4), ey - L * Math.sin(ang + 0.4)];
    svg += `<line class="edge ${on ? "seen" : ""}" x1="${sx}" y1="${sy}" x2="${ex}" y2="${ey}" ${extra ? 'stroke-dasharray="4 3"' : ""}/>`
      + `<polygon class="arrow ${on ? "seen" : ""}" points="${ex},${ey} ${p1.join(",")} ${p2.join(",")}"/>`;
  }
  for (const [n, [x, y]] of Object.entries(NODES)) {
    svg += `<g class="node ${seen.has(n) ? "seen" : ""} ${n === now ? "now" : ""}"><rect x="${x - hw}" y="${y - hh}" width="${hw * 2}" height="${hh * 2}" rx="7"/><text x="${x}" y="${y}">${n.replace("_", " ")}</text></g>`;
  }
  return svg + "</svg>";
}

function checksTimeline(a, now) {
  const marks = [...a.plan, ...a.checks];
  if (!a.doubt && !marks.length) return "";
  const end = Math.max(600, ...marks, a.resolved != null ? a.resolved - a.started : 0) * 1.06;
  const W = 560, X = (t) => 10 + (t / end) * (W - 20);
  let s = `<svg viewBox="0 0 ${W} 44" aria-label="Status checks"><rect class="zone" x="${X(0)}" y="6" width="${X(90) - X(0)}" height="18"/>`
    + `<line class="axis" x1="${X(0)}" y1="15" x2="${X(end)}" y2="15"/><line class="start" x1="${X(0)}" y1="7" x2="${X(0)}" y2="23"/>`;
  const labels = new Set([0, 90, ...a.plan]);
  for (const t of labels) s += `<text class="tick" x="${X(t)}" y="38">${t}s</text>`;
  for (const t of a.plan) s += `<circle class="planned" cx="${X(t)}" cy="15" r="5"/>`;
  for (const t of a.checks) s += `<circle class="spent" cx="${X(t)}" cy="15" r="3.6"/>`;
  if (a.resolved == null && now - a.started < end) s += `<line class="now" x1="${X(now - a.started)}" y1="4" x2="${X(now - a.started)}" y2="26"/>`;
  if (a.resolved != null) { const x = X(a.resolved - a.started); s += `<rect class="answer" x="${x - 4}" y="11" width="8" height="8" transform="rotate(45 ${x} 15)"/>`; }
  return `<div class="checks">${s}</svg><div class="muted small">Shaded: NPCI allows no check in the first 90 s. ○ planned · ● sent · ◆ answer stored${a.resolved == null ? " · ┊ now" : ""}</div></div>`;
}

function truth(a) {
  if (a.truth === "never received") return "never received the request";
  if (a.truth === "pending") return a.truth_at != null ? `undecided; the customer answers at ${clock(a.truth_at)}` : "undecided";
  return `${a.truth}, decided ${clock(a.truth_at)}`;
}

function renderDetail(d) {
  if (!d) { $("#drawer-body").innerHTML = `<p class="muted">Payment not found.</p>`; return; }
  const scrollTop = $("#drawer").scrollTop;
  const attempts = d.attempts.map((a) => {
    const truthCls = a.status === "succeeded" ? (a.truth === "success" ? "match" : "mismatch") : a.status === "failed" ? (a.truth === "success" ? "mismatch" : "match") : "";
    return `<div class="attempt"><div class="a-top"><span class="a-id">${esc(a.id)}</span>
      <span class="a-psp"><i style="background:${RAW[a.psp]}"></i>${a.psp}</span>
      <span class="att"><span class="${a.status}${a.doubt && a.status === "pending" ? " doubt" : ""}">${a.status.toUpperCase()}</span></span></div>
      <dl>
        <dt>sent</dt><dd>${clock(a.started)}${a.resolved != null ? ` · answered ${clock(a.resolved)} (${(a.resolved - a.started).toFixed(1)} s)` : ""}</dd>
        ${a.code ? `<dt>NPCI code</dt><dd>${esc(a.code)} · ${esc(a.blame)}'s side${a.blame === "psp" ? " (counts against the PSP)" : " (not the PSP's fault)"}</dd>` : ""}
        ${a.doubt ? `<dt>in doubt</dt><dd>yes: timeout, deemed or crash, so it was checked, never resent</dd>` : ""}
        <dt>status checks</dt><dd>${a.checks.length} sent${a.plan.length ? ` · planned at ${a.plan.join(", ")} s` : ""}${a.stopped ? " · stopped after a transport error" : ""}</dd>
        <dt>PSP's truth</dt><dd class="truth ${truthCls}">${truth(a)}${a.callback_lost ? " · callback lost" : ""} <span class="muted">· only the simulator knows</span></dd>
      </dl>${checksTimeline(a, d.now)}</div>`;
  }).join("");
  const ghosts = d.unrecorded.map((u) => `<div class="attempt ghost"><div class="a-top"><span class="a-id">${esc(u.id)}</span>
      <span class="a-psp"><i style="background:${RAW[u.psp]}"></i>${u.psp}</span><span class="att"><span class="unresolved">NOT IN SETTLE'S LOG</span></span></div>
      <dl><dt>PSP's truth</dt><dd class="truth ${u.truth === "success" ? "mismatch" : ""}">${esc(u.truth)} <span class="muted">· a request the PSP received that Settle never stored</span></dd></dl></div>`).join("");
  const r = ui.replays[d.pid];
  const replay = r ? (r.identical
    ? `<span class="res ok">Identical: ${r.inputs} stored inputs gave the same ${r.replayed} events (${r.ms} ms).</span>`
      + (d.unrecorded.length ? `<span class="muted small">The log matches production rules, so this bug acted outside it: it sent requests it never recorded.</span>` : "")
    : `<span class="res bad">Differs at event ${r.diff.seq}: stored <code>${esc(r.diff.stored)}</code>, production rules give <code>${esc(r.diff.replayed)}</code>. This history was written under a planted bug.</span>`)
    : `<span class="muted small">Re-runs the ${d.inputs} stored inputs through <code>decide()</code> with production rules and compares every event.</span>`;
  const expected = d.status === "PAID" ? d.amount : 0;
  const net = d.ledger.taken - d.ledger.refunded;
  $("#drawer-body").innerHTML = `
    <div class="d-head"><div><h3>${esc(d.order)}</h3><div class="sub">${inr(d.amount)} · <span class="mono">${esc(d.pid)}</span> · created ${clock(d.created)}, held until ${clock(d.hold_until)}</div></div>
      <div style="display:flex;gap:8px;align-items:center"><span class="status st-${d.status}">${d.status.replace("_", " ")}</span><button class="close" id="close" aria-label="Close">×</button></div></div>
    ${d.violations.map((v) => `<div class="dv"><b>${v.invariant}</b> ${esc(v.name)}: ${esc(v.detail)}</div>`).join("")}
    ${d.contradicted ? `<div class="dv">The PSP contradicted itself. Neither answer was applied; the payment waits for ops.</div>` : ""}
    <div class="d-sec"><h4>State path</h4><div class="machine">${machine(d.path)}</div></div>
    <div class="d-sec"><h4>Attempts</h4>${attempts || `<p class="muted small">No attempt was made: no PSP was available.</p>`}${ghosts}</div>
    <div class="d-sec"><h4>Money at the PSPs</h4><div class="ledger"><span>taken <b>${inr(d.ledger.taken)}</b></span><span>refunded <b>${inr(d.ledger.refunded)}</b></span>
      <span>net <b class="${d.status === "PAID" || d.status === "CLOSED" || d.status === "REFUNDED" ? (net === expected ? "ok" : "bad") : ""}">${inr(net)}</b></span></div></div>
    <div class="d-sec"><h4>Event log · ${d.events.length} stored events</h4><table class="log"><tbody>
      ${d.events.map((e) => `<tr data-seq="${e.seq}"><td class="seq">${e.seq}</td><td class="t">${short(e.t)}</td><td class="k">${e.kind}</td><td>${esc(e.text)}</td></tr>`
        + (ui.raw.has(e.seq) ? `<tr class="raw"><td></td><td colspan="3">${esc(JSON.stringify(e.data))}</td></tr>` : "")).join("")}
    </tbody></table><p class="muted small">Click an event for its stored fields. There is no status column: the state above is a fold over these events.</p></div>
    <div class="d-sec"><h4>Replay</h4><div class="replay"><button id="replay">Replay through decide()</button>${replay}</div></div>`;
  $("#drawer").scrollTop = scrollTop;
}

/* ------------------------------------------------------------------ wiring */

worker.onmessage = ({ data: m }) => {
  switch (m.type) {
    case "progress": progress(m.stage); break;
    case "ready": ready(m.catalogue); break;
    case "snapshot": if (ui.catalogue) render(m.data, m.perf); break;
    case "detail": if (m.pid === ui.open) { ui.detail = m.data; renderDetail(m.data); } break;
    case "replay": ui.replays[m.pid] = m.data; if (ui.detail && ui.detail.pid === m.pid) renderDetail(ui.detail); break;
    case "reset": $("#feed").innerHTML = ""; ui.feedIds.clear(); ui.replays = {}; ui.lastInv = {}; closePayment(); break;
    case "error": showError(m.message); break;
  }
};
worker.onerror = (e) => showError(e.message || "The worker failed to start.");

function showError(msg) {
  console.error(msg);
  if (!ui.catalogue) {
    $("#load-note").innerHTML = `<span class="error">Could not start: ${esc(msg)}</span><br>This page needs a browser with WebAssembly and Web Workers (any recent Chrome, Firefox, Safari or Edge).`;
  } else {
    $("#perf").textContent = `Error: ${msg}`;
  }
}

const control = (payload) => send({ type: "control", payload });

document.addEventListener("click", (ev) => {
  const t = ev.target.closest("button, tr[data-pid], li[data-pid], li[data-inv], tr[data-seq]");
  if (!t) return;
  if (t.matches("#speed button")) {
    $$("#speed button").forEach((b) => b.classList.toggle("on", b === t));
    send({ type: "speed", value: Number(t.dataset.speed) });
  } else if (t.id === "pause") {
    ui.paused = !ui.paused;
    t.innerHTML = ui.paused ? `<svg viewBox="0 0 24 24"><path d="M7 5l12 7-12 7z"/></svg>` : `<svg viewBox="0 0 24 24"><path d="M8 5v14M16 5v14"/></svg>`;
    t.title = ui.paused ? "Resume (space)" : "Pause (space)";
    send({ type: "pause", value: ui.paused });
  } else if (t.id === "reset") {
    send({ type: "reset", seed: Math.floor(Math.random() * 1e6) });
    $$("#tries button").forEach((b) => b.classList.remove("done"));
  } else if (t.matches(".fault")) {
    control({ what: "psp", psp: t.dataset.psp, fault: t.dataset.fault, on: !t.classList.contains("on") });
  } else if (t.id === "crash") {
    control({ what: "crash" });
  } else if (t.matches("#views button")) {
    ui.view = t.dataset.view;
    $$("#views button").forEach((b) => b.classList.toggle("on", b === t));
    send({ type: "view", value: ui.view });
  } else if (t.matches("[data-try]")) {
    tryThis(t);
  } else if (t.id === "close") {
    closePayment();
  } else if (t.id === "replay") {
    send({ type: "replay", pid: ui.open });
  } else if (t.matches("tr[data-seq]") && ui.detail) {
    const seq = Number(t.dataset.seq);
    ui.raw.has(seq) ? ui.raw.delete(seq) : ui.raw.add(seq);
    renderDetail(ui.detail);
  } else if (t.matches("li[data-inv].bad")) {
    $('#views button[data-view="flagged"]').click();
    $("#payments").closest(".panel").scrollIntoView({ behavior: "smooth", block: "center" });
  } else if (t.dataset.pid) {
    openPayment(t.dataset.pid);
  }
});

function tryThis(btn) {
  const s = ui.snap;
  const which = btn.dataset.try;
  if (which === "down") control({ what: "psp", psp: "alpha", fault: "down", on: !s.psps.alpha.down });
  if (which === "slow") control({ what: "psp", psp: "alpha", fault: "slow", on: !s.psps.alpha.slow });
  if (which === "crash") control({ what: "crash" });
  if (which === "bug") control({ what: "bug", bug: s.switches.bug ? "" : "resend_on_timeout" });
  btn.classList.add("done");
  const target = { down: "#psps", slow: "#feed", crash: "#recovery", bug: "#invariants" }[which];
  $(target).closest(".panel").animate([{ borderColor: "#34d399" }, { borderColor: "#1d2631" }], { duration: 1600 });
}

$$("[data-switch]").forEach((el) => el.addEventListener("change", () => control({ what: el.dataset.switch, on: el.checked })));
$("#bug").addEventListener("change", (e) => { control({ what: "bug", bug: e.target.value }); e.target.blur(); });
$("#rate").addEventListener("input", (e) => { $("#rate-out").textContent = `${Number(e.target.value).toFixed(1)}/s`; });
$("#rate").addEventListener("change", (e) => control({ what: "rate", rate: Number(e.target.value) }));
$("#scrim").addEventListener("click", closePayment);
document.addEventListener("keydown", (e) => {
  if (e.key === "Escape" && ui.open) closePayment();
  if (e.key === " " && !["INPUT", "SELECT", "BUTTON", "TEXTAREA"].includes(document.activeElement.tagName) && ui.catalogue) { e.preventDefault(); $("#pause").click(); }
});

progress("runtime");
send({ type: "init", indexURL: new URL("pyodide/", location.href).href, sources: new URL("settle.zip", location.href).href });
