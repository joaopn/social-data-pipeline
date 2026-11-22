// The web UI's own script, and all of it (no inline script, no eval: the content policy allows neither). htmx's own settings are in the meta tag htmx-config.

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
