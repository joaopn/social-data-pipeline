// The Chat window's interactive charts: each plot card's `.vl-chart[data-vl-spec]` (a same-origin address of `GET /chat/<id>/spec/<name>`, whose spec the server has checked) is drawn with Vega-Lite through vega-embed, under the page's content policy: Vega's expression interpreter instead of eval (`ast: true`), no style element injected (`defaultStyle: false`, the tooltip's `disableDefaultStyle`; their rules are in chat.css), the export links only, and only on a card without an image (no source, no editor, which would send the spec to another site). The colours come from the page's tokens (app.css), so a chart follows the page's theme, and it is drawn again when the theme changes. A chart wider than the conversation column is scaled down to it by chat.css (hover, zoom and pan stay on the points: measured). A chart whose spec draws a y axis on a log scale gets a "Linear / Log" switch above it, which draws a copy of the spec with those scales linear. A card whose chart is drawn hides its image (`.vl-drawn`); a spec the server refused, or any failure, is one sentence in the card and the image stays. No inline code, no eval.
(function () {
  "use strict";
  var NOTE = "This chart could not be drawn: ";
  var UNREAD = "its spec could not be read";
  var NO_LIBRARY = "the chart library did not load";
  var NOT_DRAWN = "the chart library could not draw it";
  var REASON_MAX = 300;
  var NEAR = 40;
  var DEPTH = 64;
  var VIEWS = ["layer", "concat", "hconcat", "vconcat"];
  var LOG_TYPES = ["log", "symlog"];
  var drawn = [];

  function token(name) {
    return getComputedStyle(document.documentElement).getPropertyValue(name).trim();
  }

  // The page's look as Vega-Lite's config; the spec's own config wins over it.
  function theme() {
    var ink = token("--ink"), ink2 = token("--ink-2"), muted = token("--muted"), line = token("--line"), line2 = token("--line-2"), font = token("--sans");
    var category = ["--s1", "--s2", "--s3", "--warn", "--serious", "--crit", "--good", "--muted"].map(token).filter(Boolean);
    var label = { fill: ink2 };
    return {
      background: "transparent",
      font: font,
      view: { stroke: line2 },
      title: { color: ink, subtitleColor: ink2 },
      axis: { domainColor: line2, gridColor: line, tickColor: line2, labelColor: ink2, titleColor: ink2 },
      legend: { labelColor: ink2, titleColor: ink2 },
      header: { labelColor: ink2, titleColor: ink2 },
      style: { "guide-label": label, "guide-title": label },
      mark: { color: category[0] },
      range: { category: category },
      text: { color: ink2 },
      rule: { color: muted }
    };
  }

  function conversation() {
    return document.querySelector(".cw .chat");
  }

  function atBottom(chat) {
    return chat.scrollHeight - chat.scrollTop - chat.clientHeight <= NEAR;
  }

  function stemOf(address) {
    var name = decodeURIComponent(address.split("/").pop() || "chart");
    return name.replace(/\.vl\.json$/i, "") || "chart";
  }

  function say(box, reason) {
    var note = box.nextElementSibling;
    var card = box.closest(".plot");
    if (card) {
      card.classList.remove("vl-drawn");
    }
    if (note && note.classList.contains("vl-note")) {
      var image = card && card.querySelector(".chat-image");
      note.textContent = NOTE + String(reason).slice(0, REASON_MAX).trim().replace(/[.;]+$/, "") + (image ? "; the image is below." : ".");
      note.hidden = false;
    }
  }

  // The y scales of a spec drawn on a log axis: every `encoding.y.scale` whose type is `log` or `symlog` (a symlog axis is log-like and gets the switch too), in the spec itself, its layers, its concatenated panels and the inner spec of a facet or a repeat.
  function logScales(spec) {
    var found = [];
    (function walk(node, depth) {
      if (!node || typeof node !== "object" || depth > DEPTH) {
        return;
      }
      var y = node.encoding && node.encoding.y;
      var scale = y && y.scale;
      if (scale && typeof scale === "object" && LOG_TYPES.indexOf(scale.type) >= 0) {
        found.push(scale);
      }
      VIEWS.forEach(function (key) {
        if (Array.isArray(node[key])) {
          node[key].forEach(function (child) {
            walk(child, depth + 1);
          });
        }
      });
      walk(node.spec, depth + 1);
    })(spec, 0);
    return found;
  }

  // A copy of the checked spec whose log y scales are linear; the spec itself is never changed.
  function linearCopy(spec) {
    var copy = JSON.parse(JSON.stringify(spec));
    logScales(copy).forEach(function (scale) {
      scale.type = "linear";
    });
    return copy;
  }

  // Draws the card's chart (with linear y scales when its switch says so) into a new view in the box, hidden until it is drawn; only then is the view drawn before removed, so the card keeps its size while it is drawn again. A later draw of the same card wins over one that ends after it.
  function embed(entry) {
    var box = entry.box;
    var chat = conversation();
    var follow = chat ? atBottom(chat) : false;
    var turn = entry.turn = (entry.turn || 0) + 1;
    var view = document.createElement("div");
    view.hidden = true;
    box.appendChild(view);
    var options = {
      mode: "vega-lite",
      ast: true,
      renderer: "svg",
      defaultStyle: false,
      tooltip: { disableDefaultStyle: true, theme: "sdp" },
      // A card with the assistant's image already has its download (one row of buttons, as the mock-up draws the card); a spec alone gets the export links.
      actions: box.closest(".plot").querySelector(".chat-image") ? false : { export: true, source: false, compiled: false, editor: false },
      downloadFileName: stemOf(box.getAttribute("data-vl-spec")),
      config: theme()
    };
    return window.vegaEmbed(view, entry.linear ? linearCopy(entry.spec) : entry.spec, options).then(function (result) {
      if (turn !== entry.turn) {
        result.finalize();
        box.removeChild(view);
        return;
      }
      if (entry.result) {
        entry.result.finalize();
      }
      entry.result = result;
      while (box.firstChild !== view) {
        box.removeChild(box.firstChild);
      }
      view.hidden = false;
      box.closest(".plot").classList.add("vl-drawn");
      if (chat && follow) {
        chat.scrollTop = chat.scrollHeight;
      }
    }, function (error) {
      if (view.parentNode === box) {
        box.removeChild(view);
      }
      throw error;
    });
  }

  // The "Linear / Log" switch above a chart with a log y scale: two buttons (`aria-pressed`) that draw the card again with its log y scales linear, or as the spec has them. The choice is the card's while the page lives.
  function addSwitch(entry) {
    var group = document.createElement("div");
    group.className = "vl-scale";
    group.setAttribute("role", "group");
    group.setAttribute("aria-label", "Y axis scale");
    [["Linear", true], ["Log", false]].forEach(function (choice) {
      var button = document.createElement("button");
      button.type = "button";
      button.className = "sm";
      button.textContent = choice[0];
      button.setAttribute("aria-pressed", String(entry.linear === choice[1]));
      button.addEventListener("click", function () {
        if (entry.linear === choice[1]) {
          return;
        }
        entry.linear = choice[1];
        group.querySelectorAll("button").forEach(function (other) {
          other.setAttribute("aria-pressed", String(other === button));
        });
        embed(entry).catch(function () {
          say(entry.box, NOT_DRAWN);
        });
      });
      group.appendChild(button);
    });
    entry.box.parentNode.insertBefore(group, entry.box);
  }

  function draw(box) {
    box.setAttribute("data-vl-state", "asked");
    if (typeof window.vegaEmbed !== "function") {
      say(box, NO_LIBRARY);
      return;
    }
    // A redirect (a session that ended leads to the login) is a failure, never a spec.
    fetch(box.getAttribute("data-vl-spec"), { credentials: "same-origin", redirect: "error", headers: { "Accept": "application/json" } }).then(function (answer) {
      var type = answer.headers.get("content-type") || "";
      if (answer.ok && type.indexOf("application/json") === 0) {
        return answer.json();
      }
      if (answer.status === 422) {
        return answer.text().then(function (reason) {
          var refusal = new Error(reason || UNREAD);
          refusal.sdpReason = true;
          throw refusal;
        });
      }
      throw new Error(UNREAD);
    }).then(function (spec) {
      delete spec.usermeta;
      var entry = { box: box, spec: spec, linear: false };
      return embed(entry).then(function () {
        drawn.push(entry);
        if (logScales(spec).length) {
          addSwitch(entry);
        }
      }, function () {
        var failure = new Error(NOT_DRAWN);
        failure.sdpReason = true;
        throw failure;
      });
    }).catch(function (error) {
      // Only the server's reason or this file's own sentences are shown, never a message of the browser or the library.
      say(box, error && error.sdpReason ? error.message : UNREAD);
    });
  }

  function scan() {
    document.querySelectorAll(".vl-chart[data-vl-spec]:not([data-vl-state])").forEach(draw);
  }

  // The theme changed (the page's data-theme, or the browser's scheme while the page follows it): every chart is drawn again in the new colours.
  function redraw() {
    drawn = drawn.filter(function (entry) {
      if (!document.contains(entry.box)) {
        entry.result.finalize();
        return false;
      }
      embed(entry).catch(function () {
        say(entry.box, NOT_DRAWN);
      });
      return true;
    });
  }

  document.addEventListener("DOMContentLoaded", scan);
  document.addEventListener("htmx:afterSettle", scan);
  new MutationObserver(redraw).observe(document.documentElement, { attributes: true, attributeFilter: ["data-theme"] });
  if (window.matchMedia) {
    window.matchMedia("(prefers-color-scheme: light)").addEventListener("change", redraw);
  }
})();
