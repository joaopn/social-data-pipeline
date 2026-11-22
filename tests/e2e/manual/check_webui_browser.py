#!/usr/bin/env python3
"""A check of the web UI in a real browser, run by hand against a running page: `PYTHON tests/e2e/manual/check_webui_browser.py https://127.0.0.1:<port>/` with the management password on standard input, where PYTHON has Playwright and its Chromium, and `PLAYWRIGHT_BROWSERS_PATH` is set in the environment to the folder of its browsers (it has no default). `check_webui.py --browser PYTHON` runs it against the page it starts, whose deployment has a source `demo` whose profile `parse` runs a program that prints a line every second.

It is not collected by pytest. In headless Chromium (the self-signed certificate accepted for this one context), by clicking: it loads the login page, logs in through the form, loads the Overview and the Sources page; starts a run of `parse` from the source's page (a box of its form ticked, its button, then Confirm on the pending page, which shows the command with the flag), opens the run from its outcome, sees the output view grow by itself, cancels the run from a second tab (its pending page, Confirm) and sees the first tab's output view stop with the program's last line; confirms `db.stop` of every configured service from the Databases page; confirms a step-up action (removing the scheduler's configuration, from the Databases page's Queue setup tab) with a wrong management password first, which is refused, then with the right one; starts a second run, ends the session behind the page's back (a logout sent by a second client with the browser's cookie) and sees the page load the login page by itself at its next poll; logs in again, cancels that run and logs out through the button. It fails on any Content-Security-Policy violation (the `securitypolicyviolation` event, collected by a script Playwright adds to every page before the page's own), any console error (but the one Chromium writes for the 403 of the wrong management password, which is listed apart), any failed request of the page's own origin (a poll that a navigation of the page itself cuts short, `net::ERR_ABORTED`, is listed apart and not counted), and on a page that is not the one expected after each step. It prints what it saw. Standard library and Playwright only.
"""

import os
import ssl
import sys
import time
import urllib.parse
import urllib.request

from playwright.sync_api import sync_playwright

WATCH = """
window.__sdpViolations = [];
document.addEventListener('securitypolicyviolation', function (event) {
  window.__sdpViolations.push(event.violatedDirective + ' ' + event.blockedURI);
});
"""
WAIT_MS = 60000


def main():
    if not os.environ.get("PLAYWRIGHT_BROWSERS_PATH"):
        print("PLAYWRIGHT_BROWSERS_PATH must be set to the folder of Playwright's browsers", file=sys.stderr)
        return 2
    base = sys.argv[1]
    password = sys.stdin.readline().rstrip("\n")
    origin = "{0.scheme}://{0.netloc}".format(urllib.parse.urlsplit(base))
    problems = []
    aborted = []
    expected = []
    # Set around the one refusal the check provokes on purpose.
    state = {"expecting_refusal": False}
    seen = []
    with sync_playwright() as playwright:
        browser = playwright.chromium.launch()
        context = browser.new_context(ignore_https_errors=True)
        context.add_init_script(WATCH)

        def watch(page):
            def console(message):
                if message.type != "error":
                    return
                # The one refusal the check provokes on purpose: the confirmation with a wrong management password is answered 403, which Chromium reports as a console error of the page.
                if state["expecting_refusal"] and "status of 403" in message.text and message.location.get("url", "").endswith("/confirm"):
                    expected.append(message.text)
                    return
                problems.append(f"console {message.type}: {message.text}")

            page.on("console", console)

            def failed(request):
                if not request.url.startswith(origin):
                    return
                if request.failure == "net::ERR_ABORTED":
                    aborted.append(urllib.parse.urlsplit(request.url).path)
                else:
                    problems.append(f"request failed: {request.url} {request.failure}")

            page.on("requestfailed", failed)
            page.on("response", lambda response: seen.append((response.status, response.url)))
            return page

        page = watch(context.new_page())

        def violations(on=None):
            on = on or page
            found = on.evaluate("window.__sdpViolations || []")
            problems.extend(f"CSP violation on {on.url}: {entry}" for entry in found)
            return found

        def wait_for(on, expression, what, seconds=WAIT_MS / 1000):
            """Ask the page `expression` (through Playwright's evaluation, which the page's policy does not govern; `wait_for_function` evaluates a string with `eval` in the page, which the policy blocks) until it is true."""
            deadline = time.monotonic() + seconds
            while not on.evaluate(expression):
                if time.monotonic() > deadline:
                    raise AssertionError(f"timed out waiting for {what} on {on.url}")
                time.sleep(0.25)

        def expect(condition, what):
            if not condition:
                problems.append(what)

        def path_of(on):
            return urllib.parse.urlsplit(on.url).path

        def log_in():
            page.fill("input[name=password]", password)
            page.click("button[type=submit]")
            page.wait_for_load_state("networkidle")

        def confirm(on, management=None):
            """Click Confirm on a pending page (with the management password when given) and wait for the next page."""
            if management is not None:
                on.fill("input[name=management_password]", management)
            on.click("form[action$='/confirm'] button[type=submit]")
            on.wait_for_load_state("networkidle")

        def run_parse():
            page.goto(base + "sources/demo")
            form = "form.action-form:has(input[name=profile][value=parse])"
            page.check(form + " input[name=skip_lingua_files]")
            page.click(form + " button[type=submit]")
            page.wait_for_load_state("networkidle")
            expect(path_of(page).startswith("/pending/") and "python sdp.py run parse --source=demo --skip-lingua-files" in page.inner_text("section.dialog pre.cli"), f"the run's pending page is not shown with its command: {page.url}")
            confirm(page)
            page.wait_for_selector("a[href^='/runs/r']", timeout=WAIT_MS)
            page.click("a[href^='/runs/r']")
            page.wait_for_load_state("domcontentloaded")
            return page.url

        page.goto(base + "login")
        print(f"  login page: {page.title()!r}, htmx loaded: {page.evaluate('typeof window.htmx')}")
        violations()
        log_in()
        if path_of(page) != "/" or "Overview" not in page.title():
            problems.append(f"after the login the page is {page.url} ({page.title()!r})")
        print(f"  after the login: {page.url}, title {page.title()!r}, the source demo listed: {page.locator('a[href=\"/sources/demo\"]:text-is(\"demo\")').count() == 1}")
        violations()
        page.click("a[href='/sources']")
        page.wait_for_load_state("networkidle")
        print(f"  Sources: {page.title()!r}, the source demo linked: {page.locator('a[href=\"/sources/demo\"]').count() == 1}")
        violations()

        run_url = run_parse()
        print(f"  the run started by clicking: {path_of(page)}")
        wait_for(page, "!!document.querySelector('.output-view') && document.querySelector('.output-view').innerText.includes('line 2')", "the program's lines")
        before = len(page.inner_text(".output-view"))
        wait_for(page, f"document.querySelector('.output-view').innerText.length > {before}", "the output to grow")
        after = len(page.inner_text(".output-view"))
        print(f"  the output view grows by itself: {before} then {after} characters, the URL unchanged: {page.url == run_url}")
        expect(page.url == run_url, "the run page changed its address while its output grew")
        violations()
        second = watch(context.new_page())
        second.goto(run_url)
        second.click("form.action-form:has(input[name=action][value='run.cancel']) button[type=submit]")
        second.wait_for_load_state("networkidle")
        expect("python sdp.py runs cancel" in second.inner_text("section.dialog pre.cli"), "the cancel's pending page does not show its command")
        confirm(second)
        wait_for(second, "!document.body.innerText.includes('under way')", "the outcome")
        print(f"  the cancel, from a second tab: {second.inner_text('section.outcome').strip()!r}")
        violations(second)
        second.close()
        page.wait_for_selector(".output-end", timeout=WAIT_MS)
        end = page.inner_text(".output-end")
        last = [line for line in page.inner_text(".output-view").splitlines() if line.strip()][-2:]
        print(f"  the first tab's output view stopped by itself: {end!r}, its last lines {last}")
        expect("has ended" in end and "interrupted" in page.inner_text(".output-view"), "the output view did not stop with the program's last line after the cancel")
        violations()

        page.goto(base + "databases")
        page.click("form.action-form:has(input[name=action][value='db.stop']):has(select[name=service]) button[type=submit]")
        page.wait_for_load_state("networkidle")
        expect(page.inner_text("section.dialog pre.cli").strip() == "python sdp.py db stop", "db.stop's pending page does not show `python sdp.py db stop`")
        confirm(page)
        wait_for(page, "!document.body.innerText.includes('under way')", "the outcome")
        print(f"  db.stop confirmed: {page.inner_text('section.outcome').strip().splitlines()[0]!r}")
        violations()

        page.goto(base + "databases?tab=queue")
        page.click("form.action-form:has(input[name=action][value='db.unsetup_jobs']) button[type=submit]")
        page.wait_for_load_state("networkidle")
        pending = page.url
        state["expecting_refusal"] = True
        confirm(page, "a-wrong-made-up-password")
        state["expecting_refusal"] = False
        refused = page.inner_text("p.notice-bad[role=alert]")
        print(f"  a wrong management password: {refused!r}, still on the pending page: {page.url == pending + '/confirm' or page.url == pending}")
        expect("was wrong" in refused, "a wrong management password was not refused")
        confirm(page, password)
        wait_for(page, "!document.body.innerText.includes('under way')", "the outcome")
        print(f"  with the right one: {page.inner_text('section.outcome').strip().splitlines()[0]!r}")
        violations()

        run_url = run_parse()
        wait_for(page, "document.querySelector('.output-view').innerText.includes('line 1')", "the second run's lines")
        cookie = next(cookie["value"] for cookie in context.cookies() if cookie["name"] == "__Host-sdp_session")
        token = page.get_attribute("meta[name=sdp-form-token]", "content")
        unverified = ssl.create_default_context()
        unverified.check_hostname = False
        unverified.verify_mode = ssl.CERT_NONE
        request = urllib.request.Request(base + "logout", data=urllib.parse.urlencode({"form_token": token}).encode(), headers={"Origin": origin, "Cookie": f"__Host-sdp_session={cookie}", "Content-Type": "application/x-www-form-urlencoded"}, method="POST")

        class NoRedirect(urllib.request.HTTPRedirectHandler):
            def redirect_request(self, *args, **kwargs):
                return None

        try:
            urllib.request.build_opener(urllib.request.HTTPSHandler(context=unverified), NoRedirect()).open(request, timeout=30)
            status = 200
        except urllib.error.HTTPError as error:
            status = error.code
        print(f"  the session ended behind the page's back (a logout from a second client): {status}")
        page.wait_for_url("**/login**", timeout=WAIT_MS)
        print(f"  the page loaded the login page by itself: {urllib.parse.urlsplit(page.url).path}, title {page.title()!r}, a password field: {page.locator('input[name=password]').count() == 1}")
        expect(page.locator("input[name=password]").count() == 1 and page.locator(".output-view").count() == 0, "the login page was not loaded as a page of its own")
        violations()
        log_in()
        page.goto(run_url)
        page.click("form.action-form:has(input[name=action][value='run.cancel']) button[type=submit]")
        page.wait_for_load_state("networkidle")
        confirm(page)
        page.goto(run_url)
        page.wait_for_selector(".output-end", timeout=WAIT_MS)
        print(f"  the second run cancelled after logging in again: {page.inner_text('.output-end')!r}")
        violations()

        page.click("form.logout button")
        page.wait_for_load_state("networkidle")
        if path_of(page) != "/login":
            problems.append(f"after the logout the page is {page.url}")
        print(f"  after the logout: {page.url}, line {page.locator('p.notice').first.inner_text()!r}")
        violations()
        cookies = [cookie["name"] for cookie in context.cookies()]
        print(f"  cookies left: {cookies}")
        if cookies:
            problems.append(f"cookies left after the logout: {cookies}")
        browser.close()
    statuses = sorted({f"{status} {urllib.parse.urlsplit(url).path.split('/output')[0]}{'/output' if '/output' in url else ''}" for status, url in seen if not urllib.parse.urlsplit(url).path.startswith(("/pending/", "/actions/"))})
    print("  answers seen: " + ", ".join(statuses))
    if aborted:
        print(f"  polls cut short by a navigation of the page itself (not counted): {len(aborted)}")
    if expected:
        print(f"  the provoked refusal, reported by Chromium as a console error (not counted): {expected}")
    for problem in problems:
        print(f"  PROBLEM: {problem}")
    if problems:
        return 1
    print("  no CSP violation, no console error, no failed request")
    return 0


if __name__ == "__main__":
    sys.exit(main())
