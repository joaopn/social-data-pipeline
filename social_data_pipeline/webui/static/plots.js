// The Chat window's interactive charts: each plot card's `.vl-chart[data-vl-spec]` (a same-origin address of `GET /chat/<id>/spec/<name>`, whose spec the server has checked) is drawn with Vega-Lite through vega-embed, under the page's content policy: Vega's expression interpreter instead of eval (`ast: true`), no style element injected (`defaultStyle: false`, the tooltip's `disableDefaultStyle`; their rules are in chat.css), the export links only, and only on a card without an image (no source, no editor, which would send the spec to another site). The colours come from the page's tokens (app.css), so a chart follows the page's theme, and it is drawn again when the theme changes. A card whose chart is drawn hides its image (`.vl-drawn`); a spec the server refused, or any failure, is one sentence in the card and the image stays. No inline code, no eval.
(function () {
  "use strict";
  var NOTE = "This chart could not be drawn: ";
  var UNREAD = "its spec could not be read";
  var NO_LIBRARY = "the chart library did not load";
  var NOT_DRAWN = "the chart library could not draw it";
  var REASON_MAX = 300;
  var NEAR = 40;
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

  function embed(box, spec) {
    var chat = conversation();
    var follow = chat ? atBottom(chat) : false;
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
    return window.vegaEmbed(box, spec, options).then(function (result) {
      drawn.push({ box: box, spec: spec, result: result });
      box.closest(".plot").classList.add("vl-drawn");
      if (chat && follow) {
        chat.scrollTop = chat.scrollHeight;
      }
    });
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
      return embed(box, spec).catch(function () {
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
    var before = drawn;
    drawn = [];
    before.forEach(function (entry) {
      entry.result.finalize();
      if (!document.contains(entry.box)) {
        return;
      }
      while (entry.box.firstChild) {
        entry.box.removeChild(entry.box.firstChild);
      }
      embed(entry.box, entry.spec).catch(function () {
        say(entry.box, NOT_DRAWN);
      });
    });
  }

  document.addEventListener("DOMContentLoaded", scan);
  document.addEventListener("htmx:afterSettle", scan);
  new MutationObserver(redraw).observe(document.documentElement, { attributes: true, attributeFilter: ["data-theme"] });
  if (window.matchMedia) {
    window.matchMedia("(prefers-color-scheme: light)").addEventListener("change", redraw);
  }
})();
