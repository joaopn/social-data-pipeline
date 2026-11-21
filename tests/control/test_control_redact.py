"""Tests for `control.redact`, the best-effort overwriting of secrets in text and data that leave the broker.

The contracts pinned here:

- Every occurrence of a secret, as it is and as `quote` and `quote_plus` write it, is overwritten with as many `*` as it has bytes of UTF-8, so no offset and no length of the text changes. Occurrences that overlap are all covered. A secret shorter than 4 characters is ignored.
- A line that starts with `$ docker compose `, after optional spaces or tabs, is overwritten from the end of that prefix to the end of the line (to a bounded reach from the line's start).
- `redact_data` does the same for every string of decoded JSON, the keys included, to a bounded depth.
- A log read in pieces through `window`, `redact_bytes` and `cut` is, byte for byte, what overwriting the whole log and cutting it afterwards gives: for every offset and every size, with secrets and `$ docker compose ` lines across every cut and with characters of several bytes at the cuts.

All secrets here are made up.
"""

from __future__ import annotations

import random
import tracemalloc
from urllib.parse import quote, quote_plus

import pytest

from social_data_pipeline.control import redact
from social_data_pipeline.control.types import SecretValue

SECRET = "made-up-s3cret"
SPECIAL = "p@ss w/rd&x=1"
STARS = "*" * len(SECRET)


def refused(call):
    with pytest.raises(redact.DataTooLargeToRedact) as caught:
        call()
    return caught.value


def stars(text):
    return "*" * len(text.encode("utf-8"))


class TestText:
    def test_every_occurrence_is_overwritten_with_the_same_length(self):
        """Bug class: only the first occurrence replaced, or the secret replaced by a marker of another length, after which the offsets a client uses to read a log in pieces no longer point at the same bytes."""
        text = f"{SECRET} start, middle {SECRET}{SECRET} and the end {SECRET}"

        new, count = redact.redact_text(text, [SECRET])

        assert new == f"{STARS} start, middle {STARS}{STARS} and the end {STARS}"
        # Two occurrences that touch are one stretch of stars.
        assert len(new) == len(text) and count == 3

    def test_the_percent_encoded_forms_are_overwritten_too(self):
        """Bug class: only the plain form looked for. A password with a special character appears in a connection string or a URL in its encoded form, and would pass in clear."""
        encoded, plus = quote(SPECIAL), quote_plus(SPECIAL)
        assert len({SPECIAL, encoded, plus}) == 3
        text = f"plain {SPECIAL} | uri mongodb://admin:{encoded}@db:27017 | form password={plus}&next=1"

        new, count = redact.redact_text(text, [SPECIAL])

        assert new == f"plain {stars(SPECIAL)} | uri mongodb://admin:{stars(encoded)}@db:27017 | form password={stars(plus)}&next=1"
        assert len(new) == len(text) and count == 3

    def test_a_secret_outside_ascii_keeps_the_length_in_bytes(self):
        """Bug class: one `*` per character. A log is addressed in bytes: a secret with characters of several bytes would then shift every offset after it."""
        secret = "g\u00fcltig-\u5bc6\u7801-\u20ac"
        text = f"a {secret} b {quote(secret)} c"

        new, count = redact.redact_text(text, [secret])

        assert new == f"a {stars(secret)} b {stars(quote(secret))} c"
        assert len(new.encode()) == len(text.encode()) and count == 2
        assert len(stars(secret)) == 18 and len(secret) == 11

    @pytest.mark.parametrize("short", ["", "a", "ab", "abc", "\u20ac\u20ac\u20ac"])
    def test_a_secret_shorter_than_four_characters_is_ignored(self, short):
        """Bug class: a one-letter password turning every log into rows of stars, which hides everything and tells the reader the letter."""
        text = "abc abcabc \u20ac\u20ac\u20ac a b c"

        assert redact.redact_text(text, [short]) == (text, 0)
        assert redact.redact_text("abcd " + text, [short, "abcd"]) == ("**** " + text, 1)

    def test_occurrences_that_overlap_are_all_covered(self):
        """Bug class: replacing one secret after the other. The first replacement destroys the second secret's match and leaves its tail in clear; and a secret that overlaps itself is only caught at every other position."""
        assert redact.redact_text("xx abcdefgh xx", ["abcdef", "cdefgh"]) == ("xx ******** xx", 1)
        assert redact.redact_text("xx abcdefgh xx", ["cdefgh", "abcdef"]) == ("xx ******** xx", 1)
        assert redact.redact_text("abababab-", ["abab"]) == ("********-", 1)
        assert redact.redact_text("aaaaaaa", ["aaaa"]) == ("*******", 1)
        assert redact.redact_text("one abcd two abcdabcd", ["abcd", "bcda"]) == ("one **** two ********", 2)
        # A secret that meets itself at a shift that is neither its period (3) nor its length (5): at 0 and at 4.
        assert redact.redact_text("x aabaaabaa x", ["aabaa"]) == ("x ********* x", 1)

    @pytest.mark.parametrize("secret", ["aaaa", "    ", "abab", "aabaa"])
    def test_dense_matches_cost_memory_in_proportion_to_the_text_only(self, secret):
        """Bug class: one record per occurrence. A stored secret such as four spaces or `aaaa` then matches at almost every byte of a log with such runs, and a piece of a megabyte costs a hundred megabytes (a reply of 16 MiB, gigabytes): one request takes the broker down."""
        covered = (secret * ((1 << 20) // len(secret))).encode()
        text = covered + b"\nend"

        tracemalloc.start()
        try:
            new, count = redact.redact_bytes(text, [secret])
            peak = tracemalloc.get_traced_memory()[1]
        finally:
            tracemalloc.stop()

        assert new == b"*" * len(covered) + b"\nend" and count == 1
        assert peak < 12 * len(text)

    def test_the_coverage_is_what_marking_every_occurrence_one_by_one_gives(self):
        """Bug class: a short cut over runs of a secret that repeats itself (taking the run in steps of the secret's period) that misses an occurrence at another shift, or covers a byte past the run. Compared with the plain way, every occurrence found and marked, on many small random texts from few letters, where such runs and shifts are common."""
        def plain_way(data, secrets):
            forms = {form for value in secrets if len(value) >= 4 for form in (value.encode(), quote(value).encode(), quote_plus(value).encode())}
            covered = [False] * len(data)
            for form in forms:
                for start in range(len(data) - len(form) + 1):
                    if data[start:start + len(form)] == form:
                        covered[start:start + len(form)] = [True] * len(form)
            stretches = sum(1 for index, flag in enumerate(covered) if flag and (index == 0 or not covered[index - 1]))
            return bytes(42 if flag else byte for flag, byte in zip(covered, data)), stretches

        generator = random.Random(20261121)
        for _ in range(2000):
            letters = generator.choice(["ab", "abc", "a", "ab "])
            data = "".join(generator.choice(letters) for _ in range(generator.randint(0, 60))).encode()
            secrets = ["".join(generator.choice(letters) for _ in range(generator.randint(4, 8))) for _ in range(generator.randint(0, 3))]

            assert redact.redact_bytes(data, secrets, at_start=False) == plain_way(data, secrets), (data, secrets)

    def test_a_secret_inside_a_longer_secret_leaves_nothing_of_either(self):
        """Bug class: the shorter secret applied first, which cuts the longer one in two and leaves its other half readable."""
        text = "key=made-up-s3cret-and-more end"

        for order in ([SECRET, SECRET + "-and-more"], [SECRET + "-and-more", SECRET]):
            assert redact.redact_text(text, order) == ("key=" + "*" * 23 + " end", 1)

    def test_a_secret_at_the_very_start_and_the_very_end(self):
        """Bug class: an off-by-one at either end of the text."""
        assert redact.redact_text(SECRET, [SECRET]) == (STARS, 1)
        assert redact.redact_text(f"{SECRET} x {SECRET}", [SECRET]) == (f"{STARS} x {STARS}", 2)
        assert redact.redact_text(SECRET[:-1], [SECRET]) == (SECRET[:-1], 0)

    def test_wrapped_secrets_and_plain_ones_are_taken_alike(self):
        """Bug class: a `SecretValue` compared through its repr ("<secret>"), so that nothing is found and nothing is overwritten."""
        text = f"a {SECRET} b {SPECIAL} c"

        new, count = redact.redact_text(text, (SecretValue(SECRET), SPECIAL))

        assert new == f"a {STARS} b {stars(SPECIAL)} c" and count == 2
        assert redact.redact_text(text, iter([SecretValue(SECRET)]))[1] == 1
        assert redact.redact_text(text, []) == (text, 0)

    @pytest.mark.parametrize("secrets", [SECRET, SECRET.encode(), SecretValue(SECRET), [SECRET.encode()], [None], [12345], [SecretValue(None)]])
    def test_secrets_that_are_not_a_list_of_text_are_a_programming_error(self, secrets):
        """Bug class: one secret passed where a list is expected being read as its characters (each too short to count), or a value that is not text skipped: in both cases nothing is overwritten and nobody notices."""
        with pytest.raises(TypeError) as caught:
            redact.redact_text(f"a {SECRET} b", secrets)

        assert SECRET not in str(caught.value)

    def test_text_with_a_lone_surrogate_does_not_stop_the_overwriting(self):
        """Bug class: a reply with a string that is not well-formed (JSON can carry one) raising in the middle of redaction, after which the caller sends nothing or, worse, the unredacted text."""
        text = f"bad \ud800 text {SECRET} end"

        assert redact.redact_text(text, [SECRET, "odd\udfffsecret"]) == (f"bad \ud800 text {STARS} end", 1)
        assert redact.redact_text("x odd\udfffsecret y", ["odd\udfffsecret"]) == ("x " + "*" * 12 + " y", 1)


class Counted:
    """Counts the calls of one function of the module and still makes them."""

    def __init__(self, monkeypatch, name):
        self.calls = 0
        real = getattr(redact, name)

        def counting(*args):
            self.calls += 1
            return real(*args)

        monkeypatch.setattr(redact, name, counting)


class TestCost:
    def test_the_repeat_unit_of_a_secret_is_worked_out_once_per_call_and_only_when_it_is_found(self, monkeypatch):
        """Bug class: the repeat unit of every form worked out for every string of a reply, found or not. A long secret (its encoded form of a 1024-character password in 4-byte characters is 12288 bytes) then costs milliseconds per string, for every reply until the broker stops."""
        periods = Counted(monkeypatch, "_smallest_period")
        long_secret = "\U0001f600" * 1024
        # Long enough to be searched for SECRET, which is in none of them.
        value = {f"the key number {number}": [f"text number {number} of the reply", {"inner": "more text of the reply"}] for number in range(500)}

        redact.redact_data(value, [long_secret, SECRET])
        not_found = periods.calls
        value["the key number 7"][0] = f"a {SECRET} b"
        value["the key number 9"][0] = f"{SECRET}{SECRET}"
        new, count = redact.redact_data(value, [long_secret, SECRET])

        assert not_found == 0
        assert periods.calls == 1 and count == 2 and new["the key number 7"][0] == f"a {STARS} b"

    def test_one_call_takes_on_at_most_four_mebibytes(self):
        """Bug class: no bound on what one call takes on, so that the work of a single request has no ceiling."""
        limit = redact.MAX_REDACT_BYTES

        assert redact.redact_text("x" * limit, [SECRET])[1] == 0
        assert redact.redact_bytes(b"x" * limit, [SECRET])[1] == 0
        for call in (lambda: redact.redact_text("x" * (limit + 1), [SECRET]), lambda: redact.redact_text("\u00e9" * (limit // 2 + 1), [SECRET]), lambda: redact.redact_bytes(b"x" * (limit + 1), [SECRET]), lambda: redact.redact_data(["x" * (limit + 1)], [SECRET])):
            error = refused(call)
            assert error.code == "data_too_large_to_redact" and SECRET not in error.message
        assert limit == 4 * 1024 * 1024

    def test_the_strings_of_data_are_counted_together_and_the_walk_stops_early(self, monkeypatch):
        """Bug class: a bound per string only, which ten thousand strings of a megabyte each pass one by one; or a bound checked after the whole walk, when the work is already done."""
        redacted = Counted(monkeypatch, "_redact")
        mib = 1024 * 1024
        fitting = {"a": ["x" * mib] * 3, "b": "y" * (mib - 8)}
        value = {**fitting, "zz": ["x" * 100] * 1000}

        error = refused(lambda: redact.redact_data(value, [SECRET]))

        # One form, so each string costs its bytes and one unit. The keys "a" and "b" (two each), three strings of a MiB (one more each) and one of a MiB less eight bytes (one more) cost exactly 4 MiB; the key "zz" is the first past the bound, and none of the thousand strings after it is looked at. The two keys are too short to be searched.
        assert error.code == "data_too_large_to_redact"
        assert redacted.calls == 4
        assert redact.redact_data(fitting, [SECRET]) == (fitting, 0)

    def test_the_budget_is_the_documented_figure_to_the_unit(self):
        """Bug class: a budget that is one unit off in either direction, which every other test of the bound misses because its refusals overshoot by two or more: a string whose cost is exactly the figure passes, and one unit more is refused."""
        # One form, so a string costs its bytes and one unit.
        at_the_figure = ["x" * (redact.MAX_REDACT_COST - 1)]
        assert redact.redact_data(at_the_figure, [SECRET]) == (at_the_figure, 0)
        assert refused(lambda: redact.redact_data(["x" * redact.MAX_REDACT_COST], [SECRET])).code == "data_too_large_to_redact"
        # The same to the unit where characters and bytes differ: two-byte characters pass the check on characters made before encoding, so only the charge on the bytes can refuse the second string.
        half = redact.MAX_REDACT_COST // 2
        in_bytes = ["x" + "\u00e9" * (half - 1)]
        assert redact.redact_data(in_bytes, [SECRET]) == (in_bytes, 0)
        assert refused(lambda: redact.redact_data(["xx" + "\u00e9" * (half - 1)], [SECRET])).code == "data_too_large_to_redact"

    def test_a_piece_whose_stretch_is_too_large_is_refused_before_anything_is_read(self):
        """Bug class: the bound checked on what was read, after the read: a request for a piece of a gigabyte has already cost the read."""
        reads = []

        def read_at(offset, count):
            reads.append(count)
            return b"x" * count

        error = refused(lambda: redact.redact_piece(read_at, 10**6, redact.MAX_REDACT_BYTES, [SECRET]))
        piece, _ = redact.redact_piece(read_at, 10**6, 1024 * 1024, [SECRET])

        assert error.code == "data_too_large_to_redact" and len(piece) == 1024 * 1024
        assert len(reads) == 1 and reads[0] <= redact.MAX_REDACT_BYTES

    def test_at_most_two_hundred_fifty_six_secrets(self):
        """Bug class: no bound on the list of secrets, whose length multiplies the work of every call."""
        secrets = [f"made-up-secret-{number:04d}" for number in range(257)]

        assert redact.redact_text("made-up-secret-0255", secrets[:256]) == ("*" * 19, 1)
        error = refused(lambda: redact.redact_text("made-up-secret-0256", secrets))
        assert error.code == "data_too_large_to_redact"
        assert refused(lambda: redact.window(0, 10, secrets)).code == "data_too_large_to_redact"

    @staticmethod
    def steps(monkeypatch):
        """Count the work of the calls that follow: first searches for a form, masks made, repeat units worked out."""
        counted = {"finds": Counted(monkeypatch, "_find_first"), "periods": Counted(monkeypatch, "_smallest_period")}
        masks = {"calls": 0}

        def counting_bytearray(*args):
            masks["calls"] += 1
            return bytearray(*args)

        monkeypatch.setattr(redact, "bytearray", counting_bytearray, raising=False)
        return lambda: {"finds": counted["finds"].calls, "masks": masks["calls"], "periods": counted["periods"].calls}

    def test_millions_of_empty_strings_cost_no_search_and_are_refused_early(self, monkeypatch):
        """Bug class: a budget that counts bytes only. Empty strings add nothing to it, so millions of them pass, and each one runs a mask and a search for every form: with 256 secrets, hundreds of millions of searches for one reply."""
        steps = self.steps(monkeypatch)
        secrets = [f"made-up-{number:03d}" for number in range(256)]
        value = [""] * 3_000_000

        error = refused(lambda: redact.redact_data(value, secrets))

        assert error.code == "data_too_large_to_redact"
        assert steps() == {"finds": 0, "masks": 0, "periods": 0}
        assert value == [""] * 3_000_000

    def test_a_million_short_strings_with_many_secrets_are_refused_before_a_budget_of_searches(self, monkeypatch):
        """Bug class: the same with strings just long enough to be searched: a million strings of 4 bytes fit inside 4 MiB, and 256 secrets of 4 characters make every one of them 256 searches. Charged a unit per form, the walk stops after about sixteen thousand strings, with no more searches than the budget."""
        steps = self.steps(monkeypatch)
        secrets = [f"{number:04d}" for number in range(256)]
        value = ["abcd"] * 1_000_000

        error = refused(lambda: redact.redact_data(value, secrets))

        counted = steps()
        assert error.code == "data_too_large_to_redact"
        assert counted["finds"] == (redact.MAX_REDACT_COST // (4 + 256)) * 256 <= redact.MAX_REDACT_COST
        assert counted["masks"] == redact.MAX_REDACT_COST // (4 + 256) and counted["periods"] == 0
        assert value == ["abcd"] * 1_000_000

    def test_one_string_too_costly_by_itself_is_refused_before_it_is_searched(self, monkeypatch):
        """Bug class: the charge made after the string was redacted, so that the work the budget is for is done before the refusal."""
        steps = self.steps(monkeypatch)
        secrets = [f"made-up-{number:03d}" for number in range(256)]

        # Characters of two bytes: the count of characters fits, the bytes and the forms do not.
        error = refused(lambda: redact.redact_data({"key": "\u00e9" * (redact.MAX_REDACT_COST // 2 - 50)}, secrets))

        assert error.code == "data_too_large_to_redact"
        assert steps() == {"finds": 0, "masks": 0, "periods": 0}

    @pytest.mark.parametrize("shape", ["empty", "four bytes"])
    def test_with_few_secrets_many_small_strings_pass_up_to_the_budget_and_are_right(self, monkeypatch, shape):
        """Bug class: a budget so strict that ordinary replies of many small strings are refused, or one that is not the documented figure. With three secrets of four characters (three forms) the largest number of strings that fits passes, with every secret overwritten, and one string more is refused."""
        secrets = ["s3c1", "s3c2", "s3c3"]
        item = "" if shape == "empty" else "abcd"
        largest = redact.MAX_REDACT_COST // (len(item) + 3)
        value = [item] * largest
        if shape == "four bytes":
            value[::1000] = ["s3c2"] * len(value[::1000])
        steps = self.steps(monkeypatch)

        new, count = redact.redact_data(value, secrets)

        expected = ["****" if entry == "s3c2" else entry for entry in value]
        assert new == expected and count == len(value[::1000]) * (shape == "four bytes")
        assert steps()["masks"] == (0 if shape == "empty" else largest)
        assert refused(lambda: redact.redact_data(value + [item], secrets)).code == "data_too_large_to_redact"
        assert (shape, largest) in (("empty", 1398101), ("four bytes", 599186))

    def test_short_strings_are_not_searched_but_a_command_line_still_is(self, monkeypatch):
        """Bug class: work spent where nothing can be found (a form longer than the string, a string shorter than every form and than a command line), or the short cut taken too far, so that a short `$ docker compose ` line among long secrets is left as it is."""
        steps = self.steps(monkeypatch)
        long_secret = "made-up-" + "x" * 30

        new, count = redact.redact_data(["short", "abc " + long_secret, "$ docker compose up"], [long_secret])

        assert new == ["short", "abc " + "*" * 38, "$ docker compose **"] and count == 2
        # "short" is not looked at; the secret is looked for in the one string it fits in; the command line, shorter than the secret, still gets its mask.
        assert steps() == {"finds": 1, "masks": 2, "periods": 1}

    def test_the_worst_case_at_the_limit_takes_at_most_one_step_per_byte(self, monkeypatch):
        """Bug class: work that grows faster than the data in the case where it is largest: many different secrets of four characters against data that repeats a cycle holding all of them, each occurrence standing apart from the next of the same secret. Counted: the marks made (one step of the loop each) are at most one per byte of the 4 MiB, and every byte ends up covered."""
        marks = Counted(monkeypatch, "_mark")
        cycle = "".join(chr(code) for code in range(0x21, 0x7F))
        secrets = [(cycle * 2)[index:index + 4] for index in range(len(cycle) - 1)]
        data = (cycle * (redact.MAX_REDACT_BYTES // len(cycle) + 1)).encode()[: redact.MAX_REDACT_BYTES]

        new, count = redact.redact_bytes(data, secrets, at_start=False)

        assert new == b"*" * len(data) and count == 1
        assert 0 < marks.calls <= len(data)


class TestComposeLine:
    def test_the_command_line_is_overwritten_after_its_prefix(self):
        """Bug class: the line with which the CLI shows the compose command it runs passing as it is. It carries whatever the command line carries, in forms no list of secrets knows."""
        text = "Starting\n  $ docker compose --profile mongo run -e MONGO_ADMIN_PASSWORD=whatever mongo-ingest\nDone\n"

        new, count = redact.redact_text(text, [])

        assert new == "Starting\n  $ docker compose " + "*" * 65 + "\nDone\n"
        assert len(new) == len(text) and count == 1

    @pytest.mark.parametrize("line", ["$ docker compose up", "  $ docker compose up", "\t $ docker compose up", "$ docker compose  ", "      $ docker compose a b c"])
    def test_spaces_and_tabs_before_the_prefix_are_allowed(self, line):
        """Bug class: the rule tied to one exact indentation, which the next change of the CLI's output breaks without a test noticing."""
        prefix_end = line.index("$ docker compose ") + len("$ docker compose ")

        for text in (line, "first\n" + line + "\nlast", "first\r\n" + line + "\r\nlast", "first\r" + line):
            start = text.index(line)
            new, count = redact.redact_text(text, [])

            assert new == text[:start + prefix_end] + "*" * (len(line) - prefix_end) + text[start + len(line):]
            assert count == (1 if len(line) > prefix_end else 0)

    @pytest.mark.parametrize("line", ["x $ docker compose up", "$ docker-compose up", "$ docker compose", "$docker compose up", "# $ docker compose up", "$ Docker Compose up", "\u00a0$ docker compose up"])
    def test_other_lines_are_left_alone(self, line):
        """Bug class: a rule so loose that it blanks any line that mentions the command, which hides the log from the operator who has to read it."""
        text = "first\n" + line + "\nlast\n"

        assert redact.redact_text(text, []) == (text, 0)

    def test_a_secret_and_a_command_line_together(self):
        """Bug class: the two rules getting in each other's way: the stars of one hiding the other's match, or the count of covered bytes going wrong where they meet."""
        text = f"{SECRET}\n$ docker compose run -e P={SECRET} x\nafter {SECRET}\n"

        new, count = redact.redact_text(text, [SECRET])

        assert new == f"{STARS}\n$ docker compose " + "*" * (len(SECRET) + 11) + f"\nafter {STARS}\n"
        assert len(new) == len(text) and count == 3

    def test_what_is_covered_is_decided_on_the_text_as_it_was(self):
        """Bug class: one rule applied to what the other left behind. A secret that reaches across the end of the command line's prefix is no longer found once the line was overwritten, and its first half stays; a secret that holds a line break hides the start of the command line after it."""
        across_the_prefix = "$ docker compose made-up-s3cret and more\n"
        across_a_line_break = "the tail\n$ docker compose run x\nlast\n"

        assert redact.redact_text(across_the_prefix, ["compose made-up"]) == ("$ docker " + "*" * 31 + "\n", 1)
        assert redact.redact_text(across_a_line_break, ["tail\n$ docker"]) == ("the " + "*" * 13 + " compose " + "*" * 5 + "\nlast\n", 2)

    def test_the_rule_reaches_a_bounded_distance_from_the_start_of_the_line(self):
        """Bug class: a reach that is counted from the wrong place or cuts a character in two, which leaves bytes that are not text behind; and the default reach being so short that an ordinary command line is not covered to its end."""
        line = "  $ docker compose " + "\u20ac" * 10 + " tail"

        cut_inside_a_character = redact.redact_text(line, [], compose_reach=21)[0]
        cut_between_characters = redact.redact_text(line, [], compose_reach=22)[0]
        long_line = "$ docker compose " + "x" * 60000

        assert cut_inside_a_character == "  $ docker compose " + "***" + "\u20ac" * 9 + " tail"
        assert cut_between_characters == "  $ docker compose " + "***" + "\u20ac" * 9 + " tail"
        assert redact.redact_text(line, [], compose_reach=23)[0] == "  $ docker compose " + "******" + "\u20ac" * 8 + " tail"
        assert redact.redact_text(line, [], compose_reach=19)[0] == line
        # A character of four bytes, cut after its first byte, after its second and after its third.
        for reach in (18, 19, 20):
            assert redact.redact_text("$ docker compose \U0001f600\U0001f600 x", [], compose_reach=reach)[0] == "$ docker compose ****\U0001f600 x"
        assert redact.redact_text(long_line, [])[0] == "$ docker compose " + "*" * 60000
        assert redact.COMPOSE_LINE_BYTES == 65536

    @pytest.mark.parametrize("reach", [0, -1, True, 2.5, None])
    def test_a_reach_that_is_not_a_positive_number_is_refused(self, reach):
        """Bug class: a reach of nothing switching the rule off without anyone meaning to."""
        with pytest.raises(ValueError):
            redact.redact_text("$ docker compose up", [], compose_reach=reach)


class TestData:
    def test_every_string_of_nested_data_is_overwritten_keys_included(self):
        """Bug class: only the top level, or only the values, looked at: a secret one level down, or one that became a key (a dict keyed by what a user typed), leaves the broker in clear."""
        data = {"text": f"a {SECRET} b", "list": [SECRET, [f"x{SECRET}", {"deep": SECRET, SECRET: 1}], 5, 2.5, True, None], SECRET + "-key": "value", "number": 12, "command": "$ docker compose run x"}

        new, count = redact.redact_data(data, [SECRET])

        assert new == {"text": f"a {STARS} b", "list": [STARS, [f"x{STARS}", {"deep": STARS, STARS: 1}], 5, 2.5, True, None], STARS + "-key": "value", "number": 12, "command": "$ docker compose *****"}
        assert count == 7
        # The data that was passed in is not changed.
        assert data["text"] == f"a {SECRET} b" and data["list"][0] == SECRET

    def test_data_without_a_secret_comes_back_equal(self):
        """Bug class: redaction changing data it has no business with: numbers turned into text, booleans into numbers, the order of keys."""
        data = {"b": [1, 2.5, True, False, None, "text", {"a": {}}], "a": "", "n": 0}

        new, count = redact.redact_data(data, [SECRET])

        assert new == data and list(new) == ["b", "a", "n"] and count == 0
        assert [type(item) for item in new["b"]] == [int, float, bool, bool, type(None), str, dict]

    def test_the_depth_is_bounded_with_a_typed_error(self):
        """Bug class: a reply nested a few thousand levels deep ending redaction in a RecursionError, at which point the caller has an unredacted reply in its hands."""
        def nested(levels):
            value = SECRET
            for _ in range(levels):
                value = [value]
            return value

        assert redact.redact_data(nested(64), [SECRET])[1] == 1
        with pytest.raises(redact.DataTooDeep) as caught:
            redact.redact_data(nested(65), [SECRET])
        with pytest.raises(redact.DataTooDeep):
            redact.redact_data(nested(5000), [SECRET])
        with pytest.raises(redact.DataTooDeep):
            redact.redact_data({"a": {"b": {"c": 1}}}, [SECRET], max_depth=2)

        assert redact.redact_data({"a": {"b": 1}}, [SECRET], max_depth=2) == ({"a": {"b": 1}}, 0)
        assert caught.value.code == "data_too_deep" and SECRET not in caught.value.message

    @pytest.mark.parametrize("value", [{"a": b"bytes"}, {"a": (1, 2)}, {"a": {1, 2}}, {1: "a"}, {"a": SecretValue(SECRET)}, [object()]])
    def test_what_decoded_json_cannot_hold_is_refused(self, value):
        """Bug class: a value of another type passed through unlooked-at. Whatever text it holds would leave the broker without having been through redaction."""
        with pytest.raises(redact.DataNotJson) as caught:
            redact.redact_data(value, [SECRET])

        assert caught.value.code == "data_not_json" and SECRET not in caught.value.message


# ── a log read in pieces ────────────────────────────────────────────────────

SECRETS = ["s3cr\u00e9t!", "abab", "p w/d", "\u20ac\u20ac\u20ac\u20ac", "compose yes"]
LOG = (
    "s3cr\u00e9t! abababab \u20ac\u20ac\u20ac\u20ac\u20ac\u20ac \u00e9\u00e9s3cr\u00e9t!\u00e9\u00e9\n"
    "  $ docker compose run -e A=\u00e9\u00e9\u00e9\u00e9\u00e9\u00e9\u00e9 \u5bc6\u7801 \U0001f600 zzz\n"
    "p+w%2Fd p%20w/d s3cr%C3%A9t%21 %E2%82%AC%E2%82%AC%E2%82%AC%E2%82%AC \u20ac$ docker compose no\r$ docker compose yes s3cr\u00e9t!\n"
    "\t$ docker compose \n$ docker compose end s3cr\u00e9t!"
).encode("utf-8") + (
    # Bytes that are not UTF-8: a lone continuation byte, runs of them right after the prefix of a command line and inside a secret's neighbourhood, a lead byte without its tail, and an invalid byte.
    b"\n$ docker compose \x80\x80\x80\x80\x80\x80 x\n\xe2\x82s3cr\xc3\xa9t!\x80\x80\xff abab\xbf\xbf\xbfabab\n"
)


def read_at_of(data):
    return lambda offset, count: data[offset:offset + count]


class TestPieces:
    # In the second line of the log a reach of 29 ends inside a character of two bytes, 44 inside one of three, 51 after the first byte of one of four; 18 ends before that line's prefix does.
    @pytest.mark.parametrize("reach", [18, 29, 44, 51, 1000])
    def test_every_piece_of_every_size_is_what_the_whole_log_gives(self, reach):
        """Bug class: a secret that a piece's boundary cuts in two passing in clear, half in one piece and half in the next; a `$ docker compose ` line covered in the piece that holds its start and readable in the pieces after it; a piece that differs from the whole log by whether its first byte was taken for the start of a line. Every sequence of pieces is made of pieces, so every piece is checked: each offset with each size, down to one byte, which puts every boundary on every byte of every secret and every character."""
        whole, _ = redact.redact_bytes(LOG, SECRETS, compose_reach=reach)
        assert len(whole) == len(LOG) and whole != LOG

        wrong = []
        for offset in range(len(LOG) + 2):
            for limit in range(1, len(LOG) - offset + 3):
                piece, _ = redact.redact_piece(read_at_of(LOG), offset, limit, SECRETS, compose_reach=reach)
                if piece != whole[offset:offset + limit]:
                    wrong.append((offset, limit))

        assert wrong == []

    def test_the_whole_log_is_overwritten_as_expected(self):
        """Bug class: the comparison above passing because both sides are wrong in the same way (nothing overwritten at all)."""
        whole, count = redact.redact_bytes(LOG, SECRETS)
        text = whole.decode("utf-8", "surrogateescape")

        assert text.split("\n")[0] == "******** ******** ****************** \u00e9\u00e9********\u00e9\u00e9"
        assert text.split("\n")[1] == "  $ docker compose " + "*" * 39
        assert text.split("\n")[2] == "******* ******* ************** ************************************ \u20ac$ docker compose no\r$ docker " + "*" * 20
        assert text.split("\n")[3:6] == ["\t$ docker compose ", "$ docker compose " + "*" * 12, "$ docker compose " + "*" * 8]
        assert whole.split(b"\n")[6] == b"\xe2\x82" + b"*" * 8 + b"\x80\x80\xff ****\xbf\xbf\xbf****"
        assert count == 15
        assert not any(secret.encode() in whole for secret in SECRETS)

    def test_sequences_of_pieces_of_the_smallest_real_size_add_up_to_the_whole_log(self):
        """Bug class: the property holding for toy sizes only. Here the pieces are the smallest the broker serves and a little more, with the default reach, over a log of a few hundred kilobytes that has secrets and command lines everywhere, some of them longer than the reach."""
        secrets = [SecretValue("made-up-long-s3cret-" + "k" * 300), "s3cr\u00e9t!"]
        block = ("line of output \u00e9\u00e8 s3cr\u00e9t! " + secrets[0].reveal() + " and more\n$ docker compose run -e X=" + "\u20ac" * 700 + "\n" + "plain " * 150 + "\n").encode()
        log = block * 40 + b"   $ docker compose " + "\u5bc6".encode() * 30000 + b" the end s3cr\xc3\xa9t!\n" + block * 40
        whole, _ = redact.redact_bytes(log, secrets)

        for size in (redact.MIN_PIECE_BYTES, redact.MIN_PIECE_BYTES + 1, 5000, 65536, 100003):
            pieces, offset = [], 0
            while offset < len(log):
                pieces.append(redact.redact_piece(read_at_of(log), offset, size, secrets)[0])
                offset += size
            assert b"".join(pieces) == whole, size

        assert redact.MIN_PIECE_BYTES == 4096 and len(log) > 300000
        assert secrets[0].reveal().encode() not in whole and b"s3cr\xc3\xa9t!" not in whole
        # The long line is covered as far as the reach goes and to the end of the character the reach ends in (its 65536th byte is the second of three), and no further.
        line_start = whole.index(b"   $ docker compose ")
        assert whole[line_start + 20:line_start + 65537] == b"*" * 65517 and whole[line_start + 65537:line_start + 65540] == "\u5bc6".encode()

    def test_the_window_is_the_piece_and_its_margins(self):
        """Bug class: margins measured in characters, or from the plain form only: an encoded form is up to three times as long in bytes, and a secret cut by a boundary would then lie partly outside what is read."""
        secret = "\u20ac" * 10
        longest = len(quote(secret))
        assert longest == 90 and len(secret.encode()) == 30

        assert redact.window(1000, 500, [secret], compose_reach=40) == (1000 - 89, 89 + 500 + 89, 89)
        assert redact.window(1000, 500, [secret], compose_reach=200) == (1000 - 203, 203 + 500 + 89, 203)
        assert redact.window(1000, 500, [], compose_reach=200) == (1000 - 203, 203 + 500, 203)
        assert redact.window(50, 500, [secret], compose_reach=200) == (0, 50 + 500 + 89, 50)
        assert redact.window(0, 1, ["abc"], compose_reach=1) == (0, 1, 0)
        assert redact.window(10**6, 4096, [SECRET]) == (10**6 - 65539, 65539 + 4096 + len(SECRET) - 1, 65539)

    def test_cut_takes_exactly_the_piece(self):
        """Bug class: the margins handed out with the piece (a client would see bytes twice and count offsets wrongly), or a piece cut short."""
        data = bytes(range(100))

        assert redact.cut(data, 10, 20) == bytes(range(10, 30))
        assert redact.cut(data, 90, 20) == bytes(range(90, 100))
        assert redact.cut(data, 100, 20) == b"" and redact.cut(bytearray(data), 0, 1) == b"\x00"

    @pytest.mark.parametrize("offset, limit", [(-1, 10), (0, 0), (0, -5), (True, 10), (1.0, 10), ("0", 10), (0, None), (0, 2.0)])
    def test_an_offset_or_limit_that_is_not_a_position_is_refused(self, offset, limit):
        """Bug class: a negative offset read from the end of the file, or a limit of nothing answered with an endless series of empty pieces."""
        with pytest.raises(ValueError):
            redact.window(offset, limit, [SECRET])

    def test_a_stretch_that_does_not_begin_at_offset_zero_does_not_begin_a_line(self):
        """Bug class: the first byte of whatever was read taken for the start of a line. A piece that happens to begin at the `$` of a command quoted in the middle of a line would then be blanked, and differ from the same bytes read as part of a longer piece."""
        data = b"$ docker compose up\nnext"

        assert redact.redact_bytes(data, [], at_start=True)[0] == b"$ docker compose **\nnext"
        assert redact.redact_bytes(data, [], at_start=False)[0] == data
        assert redact.redact_bytes(b"\n" + data, [], at_start=False)[0] == b"\n$ docker compose **\nnext"
