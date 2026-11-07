(function () {
  "use strict";

  var POLL_INTERVAL_MS = 7000;

  var poolsEl = document.getElementById("pools");
  var serverIndicator = document.getElementById("server-indicator");
  var metaHost = document.getElementById("meta-host");
  var metaUptime = document.getElementById("meta-uptime");
  var metaRequests = document.getElementById("meta-requests");

  var ACTIVE_STATUSES = ["inferring", "streaming", "requested tool", "retrying"];

  function statusClass(status) {
    if (status === "idle") return "healthy";
    if (status === "cooldown") return "unhealthy";
    if (ACTIVE_STATUSES.indexOf(status) !== -1) return "active";
    return null;
  }

  function formatElapsed(minutes) {
    if (minutes === null || minutes === undefined) return "";
    if (minutes < 1) return "<1m";
    if (minutes >= 60) return ">60m";
    return minutes + "m";
  }

  function formatTps(v) {
    return v === null || v === undefined ? "—" : v.toFixed(1);
  }

  function el(tag, className, text) {
    var e = document.createElement(tag);
    if (className) e.className = className;
    if (text !== undefined) e.textContent = text;
    return e;
  }

  function setText(node, text) {
    if (node.textContent !== text) node.textContent = text;
  }

  function setClass(node, className, present) {
    if (node.classList.contains(className) !== present) {
      node.classList.toggle(className, present);
    }
  }

  function buildModelRow(model) {
    var tr = document.createElement("tr");
    tr.dataset.id = model.id;

    var tdIndicator = document.createElement("td");
    tdIndicator.className = "col-indicator";
    tdIndicator.appendChild(el("span", "indicator"));

    var tdId = el("td", "col-id mono");
    var tdModel = el("td", "col-model");
    var tdApiBase = el("td", "col-apibase mono");
    var tdTps = el("td", "col-tps mono");

    var tdStatus = document.createElement("td");
    tdStatus.className = "col-status";
    tdStatus.appendChild(el("span", "status-word"));
    tdStatus.appendChild(el("span", "status-elapsed"));

    tr.appendChild(tdIndicator);
    tr.appendChild(tdId);
    tr.appendChild(tdModel);
    tr.appendChild(tdApiBase);
    tr.appendChild(tdTps);
    tr.appendChild(tdStatus);

    updateModelRow(tr, model);
    return tr;
  }

  function updateModelRow(tr, model) {
    var cls = statusClass(model.status);

    setText(tr.querySelector(".col-id"), model.id);
    setText(tr.querySelector(".col-model"), model.model);
    setText(tr.querySelector(".col-apibase"), model.apiBase);
    setText(tr.querySelector(".col-tps"), formatTps(model.tokensPerSecond));
    setText(tr.querySelector(".status-word"), cls ? model.status : "--");
    setText(tr.querySelector(".status-elapsed"), formatElapsed(model.elapsedMinutes));

    var dot = tr.querySelector(".indicator");
    var statusWord = tr.querySelector(".status-word");
    ["healthy", "unhealthy", "active"].forEach(function (name) {
      var want = cls === name;
      setClass(dot, "indicator-" + name, want);
      setClass(statusWord, "status-" + name, want);
    });
  }

  function buildPool(pool) {
    var section = el("section", "pool");
    section.dataset.name = pool.name;

    var header = el("div", "pool-header");
    var left = el("div", "pool-header-left");
    left.appendChild(el("span", "chevron"));
    left.appendChild(el("span", "pool-name", pool.name));
    header.appendChild(left);
    header.appendChild(el("div", "pool-meta"));

    var body = el("div", "pool-body");
    var scroll = el("div", "table-scroll");
    var table = document.createElement("table");
    table.className = "models";

    var thead = document.createElement("thead");
    var headRow = document.createElement("tr");
    [
      ["", "col-indicator"],
      ["ID", ""],
      ["Model", ""],
      ["API Base", ""],
      ["tok/s", "col-tps"],
      ["Status", ""]
    ].forEach(function (pair) {
      var th = document.createElement("th");
      if (pair[1]) th.className = pair[1];
      th.textContent = pair[0];
      headRow.appendChild(th);
    });
    thead.appendChild(headRow);

    var tbody = document.createElement("tbody");
    table.appendChild(thead);
    table.appendChild(tbody);
    scroll.appendChild(table);
    body.appendChild(scroll);

    section.appendChild(header);
    section.appendChild(body);

    updatePool(section, pool);
    return section;
  }

  function updatePool(section, pool) {
    var meta = section.querySelector(".pool-meta");
    var text = pool.queued + " queued";
    if (pool.batchElapsedMinutes !== null && pool.batchElapsedMinutes !== undefined) {
      text += ", " + formatElapsed(pool.batchElapsedMinutes);
    }
    setText(meta, text);

    var tbody = section.querySelector("tbody");
    var existing = {};
    Array.prototype.forEach.call(tbody.children, function (tr) {
      existing[tr.dataset.id] = tr;
    });

    var seen = {};
    (pool.models || []).forEach(function (model) {
      seen[model.id] = true;
      var tr = existing[model.id];
      if (tr) {
        updateModelRow(tr, model);
      } else {
        tbody.appendChild(buildModelRow(model));
      }
    });

    Object.keys(existing).forEach(function (id) {
      if (!seen[id]) tbody.removeChild(existing[id]);
    });
  }

  function render(data) {
    setText(metaHost, data.server.host);
    setText(metaUptime, data.server.uptime);
    setText(metaRequests, Number(data.server.totalRequests || 0).toLocaleString());

    var existing = {};
    Array.prototype.forEach.call(poolsEl.children, function (section) {
      existing[section.dataset.name] = section;
    });

    var seen = {};
    (data.pools || []).forEach(function (pool) {
      seen[pool.name] = true;
      var section = existing[pool.name];
      if (section) {
        updatePool(section, pool);
      } else {
        poolsEl.appendChild(buildPool(pool));
      }
    });

    Object.keys(existing).forEach(function (name) {
      if (!seen[name]) poolsEl.removeChild(existing[name]);
    });
  }

  function setOnline(isOnline) {
    setClass(serverIndicator, "offline", !isOnline);
    setText(serverIndicator.querySelector(".indicator-label"), isOnline ? "ONLINE" : "OFFLINE");
    setClass(serverIndicator.querySelector(".dot"), "dot-online", isOnline);
    setClass(serverIndicator.querySelector(".dot"), "dot-offline", !isOnline);
  }

  function poll() {
    fetch("status", { cache: "no-store" })
      .then(function (resp) {
        if (!resp.ok) throw new Error("bad status " + resp.status);
        return resp.json();
      })
      .then(function (data) {
        setOnline(true);
        render(data);
      })
      .catch(function () {
        setOnline(false);
      });
  }

  poolsEl.addEventListener("click", function (e) {
    var header = e.target.closest(".pool-header");
    if (!header) return;
    header.closest(".pool").classList.toggle("collapsed");
  });

  poll();
  setInterval(poll, POLL_INTERVAL_MS);
})();
