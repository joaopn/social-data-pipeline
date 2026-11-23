// The web UI's own script, and all of it but theme.js (which runs before the page is drawn; no inline script, no eval: the content policy allows neither). htmx's own settings are in the meta tag htmx-config. What it keeps per browser is in localStorage, each access wrapped: without it the page works and forgets.

// htmx sends the session's form token (from the page's meta tag) with every request that is not a GET.
document.addEventListener("htmx:configRequest", function (event) {
  var meta = document.querySelector('meta[name="sdp-form-token"]');
  if (meta && event.detail.verb !== "get") {
    event.detail.headers["X-SDP-Form-Token"] = meta.getAttribute("content");
  }
});

// The Chat window's conversation (.cw .chat) opens scrolled to its newest event, and follows new events (an htmx swap inside it, an image that loads) only while the view is at the bottom (within 40 px), so that scrolling up to read is not interrupted. Nothing is focused.
(function () {
  var NEAR = 40;
  var follow = true;

  function conversation() {
    return document.querySelector(".cw .chat");
  }

  function atBottom(chat) {
    return chat.scrollHeight - chat.scrollTop - chat.clientHeight <= NEAR;
  }

  function keepAtBottom() {
    var chat = conversation();
    if (chat && follow) {
      chat.scrollTop = chat.scrollHeight;
    }
  }

  document.addEventListener("DOMContentLoaded", function () {
    var chat = conversation();
    if (!chat) {
      return;
    }
    chat.scrollTop = chat.scrollHeight;
    chat.addEventListener("scroll", function () {
      follow = atBottom(chat);
    }, { passive: true });
  });

  document.addEventListener("htmx:beforeSwap", function (event) {
    var chat = conversation();
    if (chat && event.detail.target && chat.contains(event.detail.target)) {
      follow = atBottom(chat);
    }
  });
  document.addEventListener("htmx:afterSwap", keepAtBottom);
  document.addEventListener("htmx:afterSettle", keepAtBottom);
  // An image of the conversation that loads after the page or a swap makes it taller: follow it while at the bottom (load does not bubble, so it is caught on its way down).
  document.addEventListener("load", function (event) {
    var chat = conversation();
    if (chat && event.target && event.target.tagName === "IMG" && chat.contains(event.target)) {
      keepAtBottom();
    }
  }, true);
})();

// The browser's storage, wrapped: a private window or blocked site data makes every read null and every write a no-op.
var sdpStore = {
  get: function (key) {
    try {
      return window.localStorage.getItem(key);
    } catch (error) {
      return null;
    }
  },
  set: function (key, value) {
    try {
      window.localStorage.setItem(key, value);
    } catch (error) {
      // Not kept: the open page still shows the change.
    }
  }
};

// The settings box of the top bar (_settings.html): the gear opens it and puts the focus on its first field; Escape closes it and gives the focus back to the gear; a click outside it closes it. Each field writes its key (data-setting) as soon as it changes; the theme is drawn at once (window.sdpLook, from theme.js).
(function () {
  document.addEventListener("DOMContentLoaded", function () {
    var button = document.getElementById("settings-button");
    var box = document.getElementById("settings-box");
    if (!button || !box) {
      return;
    }

    function show(open, focusGear) {
      box.hidden = !open;
      button.setAttribute("aria-expanded", open ? "true" : "false");
      if (open) {
        var first = box.querySelector("select, input, button");
        if (first) {
          first.focus();
        }
      } else if (focusGear) {
        button.focus();
      }
    }

    var fields = box.querySelectorAll("[data-setting]");
    Array.prototype.forEach.call(fields, function (field) {
      var key = field.getAttribute("data-setting");
      var kept = key === "sdp.theme" && window.sdpLook ? window.sdpLook.theme() : sdpStore.get(key);
      var known = Array.prototype.some.call(field.options, function (option) {
        return option.value === kept;
      });
      if (known) {
        field.value = kept;
      }
      field.addEventListener("change", function () {
        sdpStore.set(key, field.value);
        if (key === "sdp.theme" && window.sdpLook) {
          window.sdpLook.setTheme(field.value);
        }
      });
    });

    button.addEventListener("click", function () {
      show(box.hidden, false);
    });
    box.addEventListener("keydown", function (event) {
      if (event.key === "Escape") {
        event.preventDefault();
        show(false, true);
      }
    });
    button.addEventListener("keydown", function (event) {
      if (event.key === "Escape" && !box.hidden) {
        show(false, true);
      }
    });
    document.addEventListener("click", function (event) {
      if (!box.hidden && !box.contains(event.target) && !button.contains(event.target)) {
        show(false, false);
      }
    });
  });
})();

// The Queue's floating window (queue.html's dialog#job-panel): a click on a button with data-panel ("SQL · EXPLAIN"), or anywhere on a row tr.job[data-panel] but a link, a button, a field or a form and with no text selected (a click that ends a selection to copy is not a request), loads the job's body (GET of the data-panel address, htmx) into the window and opens it as a modal dialog. Escape (the dialog's own cancel), its Close button (data-close) and a click on the backdrop (a click whose target is the dialog itself: its content sits in .jpanel-in) close it; closing empties it, so that the ids of its action areas (pact-, pexp-, preview) never linger beside the list, and gives the focus back to the row's button. While it is open the list that asks for itself (the running list, .qlist with hx-trigger) does not: a fresh list could drop the row whose action area the window's answer also updates (out of band, and htmx logs an error for a missing target). Each answer of that list asks again 5 seconds after it arrived (load delay:5s), so a request held off ends the chain: the list held is asked once more when the window closes, and its answer starts the chain again.
(function () {
  var NOT_A_ROW_CLICK = "a, button, input, label, select, textarea, summary, form";
  var giveBack = null;
  var held = null;

  function parts() {
    var box = document.getElementById("job-panel");
    var body = document.getElementById("job-panel-body");
    return box && body && typeof box.showModal === "function" ? { box: box, body: body } : null;
  }

  function say(body, text) {
    var line = document.createElement("p");
    line.className = "muted";
    line.textContent = text;
    body.replaceChildren(line);
  }

  function open(address, back) {
    var found = parts();
    if (!found || !window.htmx || !address) {
      return;
    }
    giveBack = back;
    say(found.body, "Loading…");
    if (!found.box.open) {
      found.box.showModal();
    }
    window.htmx.ajax("GET", address, { target: "#job-panel-body", swap: "innerHTML" });
  }

  document.addEventListener("click", function (event) {
    var clicked = event.target;
    if (!(clicked instanceof Element)) {
      return;
    }
    var button = clicked.closest("button[data-panel]");
    if (button) {
      event.preventDefault();
      open(button.getAttribute("data-panel"), button);
      return;
    }
    var row = clicked.closest("tr.job[data-panel]");
    if (!row || clicked.closest(NOT_A_ROW_CLICK)) {
      return;
    }
    var selection = window.getSelection();
    if (selection && selection.toString() !== "") {
      return;
    }
    open(row.getAttribute("data-panel"), row.querySelector("button[data-panel]"));
  });

  document.addEventListener("DOMContentLoaded", function () {
    var found = parts();
    if (!found) {
      return;
    }
    found.box.addEventListener("click", function (event) {
      if (event.target === found.box || (event.target instanceof Element && event.target.closest("[data-close]"))) {
        found.box.close();
      }
    });
    found.box.addEventListener("close", function () {
      found.body.replaceChildren();
      var back = giveBack;
      giveBack = null;
      if (back && back.isConnected) {
        back.focus();
      }
      var list = held;
      held = null;
      if (list && list.isConnected && window.htmx) {
        window.htmx.ajax("GET", list.getAttribute("hx-get"), { source: list, target: list, swap: "outerHTML" });
      }
    });
  });

  // A body that arrives after the window was closed is not drawn; one that cannot be had is said.
  document.addEventListener("htmx:beforeSwap", function (event) {
    var found = parts();
    if (found && event.detail.target === found.body && !found.box.open) {
      event.detail.shouldSwap = false;
    }
  });
  ["htmx:responseError", "htmx:sendError"].forEach(function (name) {
    document.addEventListener(name, function (event) {
      var found = parts();
      if (found && found.box.open && event.detail.target === found.body) {
        say(found.body, "The job could not be loaded. Close this window and try again.");
      }
    });
  });

  document.addEventListener("htmx:beforeRequest", function (event) {
    var found = parts();
    var asking = event.detail.elt;
    if (found && found.box.open && asking instanceof Element && asking.matches(".qlist[hx-trigger][hx-get]")) {
      event.preventDefault();
      held = asking;
    }
  });
})();

// The Chat window's answer box (.cw form.send textarea): Enter sends the question through the form's own submission (requestSubmit: the browser's checks of the field and every check of the server stay), Shift+Enter makes a new line, an Enter that ends an input method's composition (isComposing, or keyCode 229 in browsers that report it so) does nothing, and a box holding only blanks sends nothing. A form already sent is not sent again by a second Enter.
(function () {
  document.addEventListener("keydown", function (event) {
    var box = event.target;
    if (event.key !== "Enter" || event.shiftKey || !box || box.tagName !== "TEXTAREA" || !box.matches(".cw form.send textarea")) {
      return;
    }
    if (event.isComposing || event.keyCode === 229) {
      return;
    }
    event.preventDefault();
    var form = box.form;
    if (!form || form.dataset.sending === "1" || box.value.trim() === "") {
      return;
    }
    if (form.requestSubmit) {
      form.requestSubmit();
    } else if (form.reportValidity()) {
      form.dataset.sending = "1";
      form.submit();
    }
  });
  // A page brought back from the browser's history may still carry the mark of the form it sent.
  window.addEventListener("pageshow", function () {
    Array.prototype.forEach.call(document.querySelectorAll(".cw form.send"), function (form) {
      delete form.dataset.sending;
    });
  });
  document.addEventListener("submit", function (event) {
    var form = event.target;
    if (form && form.matches && form.matches(".cw form.send")) {
      if (form.dataset.sending === "1") {
        event.preventDefault();
        return;
      }
      form.dataset.sending = "1";
    }
  });
})();

// The Chat window's right column ("This chat", chat.html): its button and the handle beside the conversation hide and show it (the grid then has two columns; the state is kept as sdp.chatPanel and drawn by theme.js before the page shows); the sandbox's facts (details.sandbox) stay open or closed as last left (sdp.sandboxOpen), and so does the left column's data in reach (details.reach-box, sdp.reachOpen), which is open unless this browser closed it last. Once a new end of a turn has come into the conversation, the turn totals are asked again (the event sdp-turn-ended on the body, which their part listens for).
(function () {
  function panelButtons() {
    return document.querySelectorAll("[data-chat-panel]");
  }

  function drawPanel(closed) {
    if (window.sdpLook) {
      window.sdpLook.setPanel(closed);
    }
    Array.prototype.forEach.call(panelButtons(), function (button) {
      button.setAttribute("aria-expanded", closed ? "false" : "true");
    });
  }

  function ends() {
    return document.querySelectorAll(".cw .chat .chat-end").length;
  }

  var seen = 0;

  document.addEventListener("DOMContentLoaded", function () {
    drawPanel(document.documentElement.dataset.chatPanel === "closed");
    Array.prototype.forEach.call(panelButtons(), function (button) {
      button.addEventListener("click", function () {
        var closed = button.getAttribute("data-chat-panel") === "close";
        sdpStore.set("sdp.chatPanel", closed ? "closed" : "open");
        drawPanel(closed);
        var other = document.querySelector(closed ? '[data-chat-panel="open"]' : '[data-chat-panel="close"]');
        if (other) {
          other.focus();
        }
      });
    });
    var facts = document.querySelector(".cw details.sandbox");
    if (facts) {
      facts.open = sdpStore.get("sdp.sandboxOpen") === "1";
      facts.addEventListener("toggle", function () {
        sdpStore.set("sdp.sandboxOpen", facts.open ? "1" : "0");
      });
    }
    var reachBox = document.querySelector(".cw details.reach-box");
    if (reachBox) {
      reachBox.open = sdpStore.get("sdp.reachOpen") !== "0";
      reachBox.addEventListener("toggle", function () {
        sdpStore.set("sdp.reachOpen", reachBox.open ? "1" : "0");
      });
    }
    seen = ends();
  });

  document.addEventListener("htmx:afterSettle", function () {
    var now = ends();
    if (now > seen) {
      seen = now;
      document.body.dispatchEvent(new CustomEvent("sdp-turn-ended"));
    }
  });
})();

// The new-chat forms (input.default-model) send this browser's default model for new chats (localStorage `sdp.defaultModel`, written by the settings box), only when it is one of the models the sandbox offers (`data-models`); otherwise the field stays empty and the chat starts on the back end's default.
(function () {
  function storedModel() {
    try {
      return window.localStorage.getItem("sdp.defaultModel");
    } catch (error) {
      return null;
    }
  }

  document.addEventListener("DOMContentLoaded", function () {
    var model = storedModel();
    if (!model) {
      return;
    }
    document.querySelectorAll("input.default-model").forEach(function (input) {
      var offered = (input.getAttribute("data-models") || "").split(" ");
      if (offered.indexOf(model) !== -1) {
        input.value = model;
      }
    });
  });
})();
