"""EmailAlerter — SMTP-delivered pipeline failure notifications.

stdlib-only (smtplib + email.message). No third-party dep needed —
SMTP is universal enough that every prod deployment has SMTP
credentials already (transactional service like Postmark / SES, or
the org's relay).

Config is a DSN-shaped URL:

  email://[user[:pass]@]host[:port][?from=...&to=...&starttls=1]

* ``user`` / ``pass`` — SMTP auth (optional; some relays auth by IP).
* ``port`` defaults to 465 if ``?starttls=0`` else 587.
* ``from`` — sender address (defaults to the auth user).
* ``to`` — comma-separated recipient list (required).
* ``starttls=0`` switches to implicit SSL (smtplib.SMTP_SSL); default
  is STARTTLS over port 587 which Postmark / SES / Gmail SMTP all
  expect.

Network errors are swallowed so a flaky SMTP relay can't take down
the orchestrator — same pattern as :class:`SlackAlerter`.
"""
from __future__ import annotations

import smtplib
import ssl
import sys
from email.message import EmailMessage

from . import AlertEvent


class EmailAlerter:
    def __init__(
        self,
        host: str,
        port: int,
        *,
        username: str | None = None,
        password: str | None = None,
        from_addr: str | None = None,
        to: list[str],
        use_starttls: bool = True,
        timeout: float = 10.0,
    ):
        if not host:
            raise ValueError("EmailAlerter: host is required")
        if not to:
            raise ValueError("EmailAlerter: at least one to= recipient required")
        self.host = host
        self.port = port
        self.username = username
        self.password = password
        self.from_addr = from_addr or username or "ematix-flow@localhost"
        self.to = list(to)
        self.use_starttls = use_starttls
        self._timeout = timeout

    def notify(self, event: AlertEvent) -> None:
        subject, body = self._format(event)
        msg = EmailMessage()
        msg["Subject"] = subject
        msg["From"] = self.from_addr
        msg["To"] = ", ".join(self.to)
        msg.set_content(body)

        try:
            if self.use_starttls:
                with smtplib.SMTP(self.host, self.port, timeout=self._timeout) as smtp:
                    smtp.starttls(context=ssl.create_default_context())
                    if self.username:
                        smtp.login(self.username, self.password or "")
                    smtp.send_message(msg)
            else:
                # Implicit-SSL path (port 465 is the convention).
                with smtplib.SMTP_SSL(
                    self.host, self.port, timeout=self._timeout,
                    context=ssl.create_default_context(),
                ) as smtp:
                    if self.username:
                        smtp.login(self.username, self.password or "")
                    smtp.send_message(msg)
        except (smtplib.SMTPException, OSError) as e:
            # Match SlackAlerter's swallow-and-log policy. A bad SMTP
            # endpoint shouldn't take the orchestrator loop down.
            print(
                f"warning: EmailAlerter failed to send: "
                f"{type(e).__name__}: {e}",
                file=sys.stderr,
            )

    def _format(self, event: AlertEvent) -> tuple[str, str]:
        """Subject + plain-text body. Plain-text only — no HTML — so
        the alert renders identically across every mail client and
        keeps the implementation tight."""
        ts = event.timestamp.replace(microsecond=0).isoformat().replace("+00:00", "Z")
        if event.kind == "gave_up":
            subject = (
                f"[ematix-flow] {event.pipeline} gave up "
                f"({event.attempt_count}/{event.max_attempts})"
            )
        elif event.kind == "failed":
            subject = (
                f"[ematix-flow] {event.pipeline} failed "
                f"(attempt {event.attempt_count}/{event.max_attempts})"
            )
        elif event.kind == "recovered":
            subject = f"[ematix-flow] {event.pipeline} recovered"
        else:
            subject = f"[ematix-flow] {event.pipeline} {event.kind}"

        body_lines = [
            f"Pipeline: {event.pipeline}",
            f"Event:    {event.kind}",
            f"Time:     {ts}",
        ]
        if event.error_type or event.error_message:
            body_lines.append(f"Error:    {event.error_type}: {event.error_message}")
        if event.attempt_count:
            body_lines.append(
                f"Attempts: {event.attempt_count}/{event.max_attempts}"
            )
        return subject, "\n".join(body_lines) + "\n"


def from_url(url: str) -> EmailAlerter:
    """Build an :class:`EmailAlerter` from an ``email://`` URL.

    See module docstring for the URL grammar.
    """
    from urllib.parse import parse_qs, urlparse

    parsed = urlparse(url)
    if parsed.scheme != "email":
        raise ValueError(
            f"EmailAlerter.from_url: expected scheme 'email', got {parsed.scheme!r}"
        )
    qs = {k: v[0] for k, v in parse_qs(parsed.query).items()}
    use_starttls = qs.get("starttls", "1") != "0"
    default_port = 587 if use_starttls else 465
    port = parsed.port or default_port
    if not parsed.hostname:
        raise ValueError(f"EmailAlerter.from_url: hostname missing in {url!r}")
    to_field = qs.get("to", "")
    to_list = [addr.strip() for addr in to_field.split(",") if addr.strip()]
    if not to_list:
        raise ValueError(
            f"EmailAlerter.from_url: ?to=address,address required in {url!r}"
        )
    return EmailAlerter(
        host=parsed.hostname,
        port=port,
        username=parsed.username,
        password=parsed.password,
        from_addr=qs.get("from"),
        to=to_list,
        use_starttls=use_starttls,
    )
