// What this browser keeps of the page's look, applied before the page is drawn (loaded without defer in the head, so that the page never flashes the other theme or the Chat window's hidden column). No inline code and no eval. Each read of the browser's storage is wrapped: without it (a private window, blocked storage) the page is dark with every column shown.
//   sdp.theme      "dark" (the default), "light" or "system" (follow the browser's prefers-color-scheme, also while the page is open)
//   sdp.chatPanel  "closed" hides the Chat window's right column ("This chat")
// app.js changes both through window.sdpLook when the settings box or a column's button is used.
(function () {
  var root = document.documentElement;
  var media = window.matchMedia ? window.matchMedia("(prefers-color-scheme: light)") : null;
  var choice = "dark";

  function read(key) {
    try {
      return window.localStorage.getItem(key);
    } catch (error) {
      return null;
    }
  }

  function draw() {
    root.dataset.theme = choice === "light" || (choice === "system" && media && media.matches) ? "light" : "dark";
  }

  function setTheme(value) {
    choice = value === "light" || value === "system" ? value : "dark";
    draw();
  }

  function setPanel(closed) {
    if (closed) {
      root.dataset.chatPanel = "closed";
    } else {
      delete root.dataset.chatPanel;
    }
  }

  setTheme(read("sdp.theme"));
  setPanel(read("sdp.chatPanel") === "closed");
  if (media) {
    var redraw = function () {
      if (choice === "system") {
        draw();
      }
    };
    if (media.addEventListener) {
      media.addEventListener("change", redraw);
    } else if (media.addListener) {
      media.addListener(redraw);
    }
  }
  window.sdpLook = { setTheme: setTheme, setPanel: setPanel, theme: function () { return choice; } };
})();
