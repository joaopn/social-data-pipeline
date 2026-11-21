// The web UI's own script, and all of it: htmx sends the session's form token (from the page's meta tag) with every request that is not a GET. htmx's own settings are in the meta tag htmx-config.
document.addEventListener("htmx:configRequest", function (event) {
  var meta = document.querySelector('meta[name="sdp-form-token"]');
  if (meta && event.detail.verb !== "get") {
    event.detail.headers["X-SDP-Form-Token"] = meta.getAttribute("content");
  }
});
