// What this browser keeps of the page's look, applied before the page is drawn (loaded without defer in the head, so that the page never flashes the other theme or the Chat window's columns in another state or width). No inline code and no eval. Each read of the browser's storage is wrapped: without it (a private window, blocked storage) the page is dark with every column shown at its default width.
//   sdp.theme           "dark" (the default), "light", a named theme ("nord", "dracula", "gruvbox-dark", "solarized-light", "catppuccin-latte", "nord-light"; drawn as data-theme with that name) or "system" (follow the browser's prefers-color-scheme, also while the page is open: only dark or light); any other value is dark
//   sdp.chatPanel       "closed" hides the Chat window's right column ("This chat"): data-chat-panel="closed" on the root
//   sdp.chatLeft        "closed" hides the Chat window's left column (the chats): data-chat-left="closed" on the root
//   sdp.chatLeftWidth   the left column's width in whole pixels (180 to 480; else ignored, and the default 236 holds): --chat-left-w on the root
//   sdp.chatRightWidth  the right column's width in whole pixels (260 to 640; else ignored, and the default 330 holds): --chat-right-w on the root
// The widths are set only through the root's style object (setProperty): the content policy blocks a written style attribute. chat.css uses all of this only above 1150 px wide. app.js changes it through window.sdpLook when the settings box, a column's button or a column's edge is used.
(function () {
  var root = document.documentElement;
  var media = window.matchMedia ? window.matchMedia("(prefers-color-scheme: light)") : null;
  var themes = ["dark", "light", "nord", "dracula", "gruvbox-dark", "solarized-light", "catppuccin-latte", "nord-light", "system"];
  var choice = "dark";
  var columns = {
    left: { property: "--chat-left-w", key: "sdp.chatLeftWidth", min: 180, max: 480, standard: 236, width: null },
    right: { property: "--chat-right-w", key: "sdp.chatRightWidth", min: 260, max: 640, standard: 330, width: null }
  };

  function read(key) {
    try {
      return window.localStorage.getItem(key);
    } catch (error) {
      return null;
    }
  }

  function draw() {
    if (choice === "system") {
      root.dataset.theme = media && media.matches ? "light" : "dark";
    } else {
      root.dataset.theme = choice;
    }
  }

  function setTheme(value) {
    choice = themes.indexOf(value) >= 0 ? value : "dark";
    draw();
  }

  function setPanel(closed) {
    if (closed) {
      root.dataset.chatPanel = "closed";
    } else {
      delete root.dataset.chatPanel;
    }
  }

  function setLeft(closed) {
    if (closed) {
      root.dataset.chatLeft = "closed";
    } else {
      delete root.dataset.chatLeft;
    }
  }

  // A width within the column's range in whole pixels, or null for its default.
  function setWidth(side, value) {
    var column = columns[side];
    if (!column) {
      return;
    }
    if (typeof value === "number" && Math.floor(value) === value && value >= column.min && value <= column.max) {
      column.width = value;
      root.style.setProperty(column.property, value + "px");
    } else {
      column.width = null;
      root.style.removeProperty(column.property);
    }
  }

  function storedWidth(side) {
    var text = read(columns[side].key);
    return text !== null && /^[0-9]{1,4}$/.test(text) ? Number(text) : null;
  }

  function column(side) {
    var found = columns[side];
    return { key: found.key, min: found.min, max: found.max, standard: found.standard, width: found.width === null ? found.standard : found.width };
  }

  setTheme(read("sdp.theme"));
  setPanel(read("sdp.chatPanel") === "closed");
  setLeft(read("sdp.chatLeft") === "closed");
  setWidth("left", storedWidth("left"));
  setWidth("right", storedWidth("right"));
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
  window.sdpLook = { setTheme: setTheme, setPanel: setPanel, setLeft: setLeft, setWidth: setWidth, column: column, theme: function () { return choice; } };
})();
