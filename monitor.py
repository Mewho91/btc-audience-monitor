#!/usr/bin/env python3
"""Public YouTube subscriber time-series tracker (one daily sample, Europe/Warsaw)."""
from __future__ import annotations

import argparse
import csv
import html
import json
import math
import os
import re
import sqlite3
import sys
from collections import defaultdict
from contextlib import closing
from datetime import datetime, timedelta, timezone
from pathlib import Path
from urllib.error import HTTPError, URLError
from urllib.parse import parse_qs, urlencode, urlparse
from urllib.request import Request, urlopen
from zoneinfo import ZoneInfo

ROOT = Path(__file__).resolve().parent
DATA_DIR = ROOT / "data"
DB_PATH = DATA_DIR / "monitor.sqlite"
CONFIG_PATH = ROOT / "config.json"
TZ = ZoneInfo("Europe/Warsaw")
BASE_URL = "https://www.googleapis.com/youtube/v3"


def get_api_key() -> str:
    key = os.getenv("YOUTUBE_API_KEY", "").strip()
    if key:
        return key
    env_path = ROOT / ".env"
    if env_path.exists():
        for line in env_path.read_text(encoding="utf-8-sig").splitlines():
            line = line.strip()
            if line.startswith("YOUTUBE_API_KEY="):
                return line.split("=", 1)[1].strip().strip('"').strip("'")
    raise RuntimeError("Brak klucza API. Utwórz plik .env z YOUTUBE_API_KEY=... lub ustaw zmienną środowiskową.")


def config() -> dict:
    if not CONFIG_PATH.exists():
        raise RuntimeError("Brak pliku config.json.")
    data = json.loads(CONFIG_PATH.read_text(encoding="utf-8"))
    if not data.get("channel") or data["channel"].startswith("WPISZ_"):
        raise RuntimeError("Ustaw 'channel' w pliku config.json (np. @GoogleDevelopers lub pełny URL kanału).")
    return data


def parse_channel(value: str) -> dict[str, str]:
    """Support channel IDs, handles, /channel/, /@, and legacy /user/ URLs."""
    value = value.strip().rstrip("/")
    if value.startswith("UC") and re.fullmatch(r"UC[A-Za-z0-9_-]{22}", value):
        return {"id": value}
    if value.startswith("@"):
        return {"forHandle": value}
    if "youtube.com" in value or "youtu.be" in value:
        parsed = urlparse(value if "://" in value else "https://" + value)
        chunks = [x for x in parsed.path.split("/") if x]
        if len(chunks) >= 2 and chunks[0] == "channel":
            return {"id": chunks[1]}
        if chunks and chunks[0].startswith("@"):
            return {"forHandle": chunks[0]}
        if len(chunks) >= 2 and chunks[0] == "user":
            return {"forUsername": chunks[1]}
        raise ValueError("Podaj adres /@nazwa lub /channel/UC... albo identyfikator kanału UC...")
    return {"forHandle": "@" + value.lstrip("@")}


def api_get(resource: str, params: dict, api_key: str) -> dict:
    args = {**params, "key": api_key}
    req = Request(BASE_URL + "/" + resource + "?" + urlencode(args),
                  headers={"User-Agent": "YT-Subscriber-Monitor/1.0"})
    try:
        with urlopen(req, timeout=30) as response:
            return json.load(response)
    except HTTPError as error:
        detail = error.read().decode("utf-8", errors="replace")
        try:
            payload = json.loads(detail)
            message = payload.get("error", {}).get("message", detail)
        except ValueError:
            message = detail[:500]
        raise RuntimeError(f"YouTube API ({error.code}): {message}") from error
    except URLError as error:
        raise RuntimeError(f"Błąd połączenia z YouTube: {error.reason}") from error


def database(path: Path = DB_PATH) -> sqlite3.Connection:
    path.parent.mkdir(parents=True, exist_ok=True)
    conn = sqlite3.connect(path)
    conn.execute("PRAGMA foreign_keys=ON")
    conn.executescript("""
        CREATE TABLE IF NOT EXISTS channels (
            channel_id TEXT PRIMARY KEY,
            title TEXT NOT NULL,
            channel_url TEXT NOT NULL
        );
        CREATE TABLE IF NOT EXISTS snapshots (
            channel_id TEXT NOT NULL,
            local_date TEXT NOT NULL,
            checked_at_utc TEXT NOT NULL,
            subscriber_count INTEGER NOT NULL,
            view_count INTEGER,
            video_count INTEGER,
            PRIMARY KEY (channel_id, local_date),
            FOREIGN KEY(channel_id) REFERENCES channels(channel_id)
        );
        CREATE TABLE IF NOT EXISTS videos (
            video_id TEXT PRIMARY KEY,
            channel_id TEXT NOT NULL,
            title TEXT NOT NULL,
            published_at_utc TEXT NOT NULL,
            FOREIGN KEY(channel_id) REFERENCES channels(channel_id)
        );
        CREATE INDEX IF NOT EXISTS idx_video_channel_date
        ON videos(channel_id, published_at_utc);
    """)
    return conn


def save_channel(conn: sqlite3.Connection, channel: dict) -> None:
    conn.execute("""INSERT INTO channels(channel_id, title, channel_url) VALUES(?,?,?)
        ON CONFLICT(channel_id) DO UPDATE SET title=excluded.title, channel_url=excluded.channel_url""",
                 (channel["id"], channel["snippet"]["title"], "https://www.youtube.com/channel/" + channel["id"]))


def save_snapshot(conn: sqlite3.Connection, channel: dict, now: datetime) -> None:
    stats = channel.get("statistics", {})
    if stats.get("hiddenSubscriberCount", False) or "subscriberCount" not in stats:
        raise RuntimeError("Kanał nie udostępnia publicznej liczby subskrybentów.")
    conn.execute("""INSERT INTO snapshots(channel_id, local_date, checked_at_utc, subscriber_count, view_count, video_count)
        VALUES(?,?,?,?,?,?) ON CONFLICT(channel_id, local_date) DO UPDATE SET
        checked_at_utc=excluded.checked_at_utc, subscriber_count=excluded.subscriber_count,
        view_count=excluded.view_count, video_count=excluded.video_count""",
        (channel["id"], now.astimezone(TZ).date().isoformat(), now.astimezone(timezone.utc).isoformat(),
         int(stats["subscriberCount"]), int(stats["viewCount"]) if "viewCount" in stats else None,
         int(stats["videoCount"]) if "videoCount" in stats else None))


def ingest_videos(conn: sqlite3.Connection, channel_id: str, playlist_id: str,
                  api_key: str, lookback_days: int, now: datetime, max_pages: int = 10) -> int:
    cutoff = now.astimezone(timezone.utc) - timedelta(days=lookback_days)
    token = None
    count = 0
    for _ in range(max_pages):
        params = {"part": "snippet,contentDetails", "playlistId": playlist_id, "maxResults": 50}
        if token:
            params["pageToken"] = token
        result = api_get("playlistItems", params, api_key)
        older = False
        for item in result.get("items", []):
            detail = item.get("contentDetails", {})
            video_id = detail.get("videoId") or item.get("snippet", {}).get("resourceId", {}).get("videoId")
            published = detail.get("videoPublishedAt")
            if not video_id or not published:
                continue
            publish_dt = datetime.fromisoformat(published.replace("Z", "+00:00"))
            if publish_dt < cutoff:
                older = True
                continue
            title = item.get("snippet", {}).get("title") or "Film bez tytułu"
            conn.execute("""INSERT INTO videos(video_id, channel_id, title, published_at_utc)
                VALUES(?,?,?,?) ON CONFLICT(video_id) DO UPDATE SET
                title=excluded.title, published_at_utc=excluded.published_at_utc""",
                (video_id, channel_id, title, publish_dt.astimezone(timezone.utc).isoformat()))
            count += 1
        token = result.get("nextPageToken")
        if not token or older:
            break
    else:
        print("Uwaga: osiągnięto limit 10 stron publikacji; bardzo aktywny kanał może wymagać większego limitu.")
    return count


def collect(db_path: Path = DB_PATH) -> str:
    c = config()
    key = get_api_key()
    filter_args = parse_channel(c["channel"])
    result = api_get("channels", {"part": "snippet,statistics,contentDetails", **filter_args}, key)
    channels = result.get("items", [])
    if not channels:
        raise RuntimeError("Nie znaleziono kanału. Sprawdź nazwę lub URL w config.json.")
    channel = channels[0]
    playlist_id = channel.get("contentDetails", {}).get("relatedPlaylists", {}).get("uploads")
    now = datetime.now(timezone.utc)
    with closing(database(db_path)) as conn, conn:
        save_channel(conn, channel)
        save_snapshot(conn, channel, now)
        video_count = ingest_videos(conn, channel["id"], playlist_id, key,
                                    max(1, int(c.get("video_lookback_days", 35))), now) if playlist_id else 0
    print(f"Zapisano {now.astimezone(TZ):%Y-%m-%d %H:%M} | {channel['snippet']['title']} | "
          f"{channel['statistics']['subscriberCount']} subskrybentów (wartość zaokrąglona), "
          f"{video_count} publikacji w oknie wyszukiwania")
    return channel["id"]


def get_data(conn: sqlite3.Connection, channel_id: str | None):
    if not channel_id:
        match = conn.execute("SELECT channel_id FROM channels ORDER BY rowid DESC LIMIT 1").fetchone()
        if not match:
            return None
        channel_id = match[0]
    conn.row_factory = sqlite3.Row
    chan = conn.execute("SELECT * FROM channels WHERE channel_id=?", (channel_id,)).fetchone()
    if not chan:
        return None
    snaps = conn.execute("SELECT * FROM snapshots WHERE channel_id=? ORDER BY local_date", (channel_id,)).fetchall()
    videos = conn.execute("SELECT * FROM videos WHERE channel_id=? ORDER BY published_at_utc", (channel_id,)).fetchall()
    return dict(chan), [dict(x) for x in snaps], [dict(v) for v in videos]


def exported_rows(snaps: list[dict]):
    previous = None
    for row in snaps:
        current = row["subscriber_count"]
        yield [row["local_date"], row["checked_at_utc"], current,
               "" if previous is None else current - previous,
               row.get("view_count") if row.get("view_count") is not None else "",
               row.get("video_count") if row.get("video_count") is not None else ""]
        previous = current


def render(db_path: Path = DB_PATH, html_path: Path | None = None,
           csv_path: Path | None = None, demo: bool = False, channel_id: str | None = None) -> None:
    """Ciemny dashboard BTC: dzienne wartości i pionowe linie premier filmów."""
    import plotly.graph_objects as go
    from plotly.subplots import make_subplots

    html_path = html_path or ROOT / "index.html"
    csv_path = csv_path or ROOT / "export.csv"
    with closing(database(db_path)) as conn:
        dataset = get_data(conn, channel_id)
    if not dataset:
        raise RuntimeError("Brak danych. Wykonaj pierwszy odczyt: python monitor.py --collect")
    channel, samples, videos = dataset
    if not samples:
        raise RuntimeError("Brak odczytów subskrybentów.")

    start_date = datetime.fromisoformat(samples[0]["local_date"]).date()
    end_date = datetime.fromisoformat(samples[-1]["local_date"]).date()
    dates = [s["local_date"] for s in samples]
    counts = [s["subscriber_count"] for s in samples]
    deltas = [None] + [counts[i] - counts[i - 1] for i in range(1, len(counts))]
    growth = counts[-1] - counts[0]
    days_since_start = (end_date - start_date).days

    def spaced(value: int) -> str:
        return f"{value:,}".replace(",", " ")

    # Publikacje odnoszą się do faktycznej daty udostępnienia filmu (czas PL).
    grouped = defaultdict(list)
    for video in videos:
        pub = datetime.fromisoformat(video["published_at_utc"]).astimezone(TZ)
        grouped[pub.date().isoformat()].append({
            **video,
            "local_date": pub.date().isoformat(),
            "local_time": pub.strftime("%d.%m.%Y %H:%M"),
        })
    chart_publications = {day: items for day, items in grouped.items()
                          if start_date.isoformat() <= day <= end_date.isoformat()}

    marker_hovers = []
    for day in dates:
        published = grouped.get(day, [])
        if published:
            names = "<br>".join("• " + html.escape(v["title"][:100]) for v in published[:5])
            if len(published) > 5:
                names += f"<br>... i {len(published) - 5} kolejnych"
            marker_hovers.append(f"<span style='color:#ffb84d'><b>FILMY: {len(published)}</b></span><br>{names}")
        else:
            marker_hovers.append("Brak zarejestrowanych publikacji tego dnia")

    figure = make_subplots(
        rows=2, cols=1, shared_xaxes=True, vertical_spacing=0.14,
        row_heights=[0.79, 0.21],
    )
    figure.add_trace(go.Scatter(
        x=dates,
        y=counts,
        mode="lines+markers+text",
        name="Liczba subskrybentów",
        line=dict(color="#3d96ff", width=3.4, shape="linear"),
        marker=dict(color="#65b9ff", size=8, line=dict(color="#0c1a31", width=2)),
        text=[spaced(v) for v in counts],
        textposition="top center",
        textfont=dict(color="#b6d9ff", size=11, family="Arial"),
        cliponaxis=False,
        customdata=[
            ["—" if delta is None else f"{delta:+,}".replace(",", " "), marker_hovers[i]]
            for i, delta in enumerate(deltas)
        ],
        hovertemplate=(
            "<b>%{x|%d.%m.%Y}</b>"
            "<br>Subskrybenci: <b>%{y:,.0f}</b>"
            "<br>Zmiana: <b>%{customdata[0]}</b>"
            "<br>%{customdata[1]}<extra></extra>"
        ),
    ), row=1, col=1)

    changes = deltas[1:]
    figure.add_trace(go.Bar(
        x=dates[1:], y=changes,
        name="Zmiana od odczytu",
        marker_color=["#28b49c" if v >= 0 else "#f26d81" for v in changes],
        opacity=0.9,
        hovertemplate="<b>%{x|%d.%m.%Y}</b><br>Zmiana od poprzedniego pomiaru: <b>%{y:+,.0f}</b><extra></extra>",
    ), row=2, col=1)

    # Publikacje: PIONOWE PRZERYWANE LINIE przez całą wysokość wykresu głównego.
    # Markery przy górnej krawędzi mają niezależną, ukrytą skalę y3: nie są odczytami subskrypcji.
    if chart_publications:
        pub_dates = sorted(chart_publications)
        pub_tooltips = []
        for day in pub_dates:
            items = chart_publications[day]
            details = [f"<b>Nowe filmy: {len(items)}</b>", f"{day[8:10]}.{day[5:7]}.{day[:4]}"]
            details += [f"• {html.escape(v['title'][:110])}" for v in items[:8]]
            if len(items) > 8:
                details.append(f"... i {len(items) - 8} kolejnych")
            pub_tooltips.append("<br>".join(details))
            figure.add_shape(
                type="line", xref="x", yref="y domain",
                x0=day, x1=day, y0=0, y1=1,
                line=dict(color="#ffaf45", width=1.9, dash="dash"), opacity=0.86,
                layer="above",
            )
        figure.add_trace(go.Scatter(
            x=pub_dates,
            y=[0.97] * len(pub_dates),
            yaxis="y3",
            name="Publikacja filmu",
            mode="markers",
            marker=dict(color="#ffb347", symbol="triangle-down", size=12,
                        line=dict(color="#261b18", width=1)),
            text=pub_tooltips,
            hovertemplate="%{text}<extra></extra>",
            showlegend=True,
        ))

    # 14 ostatnich dni są pokazywane od razu, aby napisy liczb nie nakładały się.
    # Przycisk 'Całość' i przybliżanie pozwalają zobaczyć wszystkie odczyty.
    initial_left = (max(start_date, end_date - timedelta(days=13)) - timedelta(days=1)).isoformat()
    initial_right = (end_date + timedelta(days=1)).isoformat()
    figure.update_layout(
        height=660,
        margin=dict(l=64, r=25, t=65, b=48),
        autosize=True,
        font=dict(family="Arial, sans-serif", color="#d4e4f8", size=12),
        paper_bgcolor="#101b2d",
        plot_bgcolor="#101b2d",
        showlegend=True,
        barmode="relative",
        hovermode="closest",
        hoverlabel=dict(bgcolor="#172840", bordercolor="#335071",
                        font=dict(color="#eaf5ff", size=12)),
        legend=dict(orientation="h", y=1.15, x=0, bgcolor="rgba(0,0,0,0)",
                    font=dict(color="#9eb4d2", size=11)),
        yaxis3=dict(overlaying="y", anchor="x", side="right", visible=False,
                    range=[0, 1], fixedrange=True),
    )
    figure.update_xaxes(
        type="date", showgrid=True, gridcolor="#1b2d47", griddash="dot",
        linecolor="#2d3e57", tickcolor="#385273",
        tickfont=dict(color="#8ba4c5", size=11),
        tickformat="%d.%m", showspikes=True, spikecolor="#5c7796",
    )
    figure.update_xaxes(
        row=2, col=1, range=[initial_left, initial_right],
        rangeselector=dict(
            buttons=[
                dict(count=14, label="14 dni", step="day", stepmode="backward"),
                dict(count=30, label="30 dni", step="day", stepmode="backward"),
                dict(count=90, label="90 dni", step="day", stepmode="backward"),
                dict(step="all", label="Całość"),
            ],
            bgcolor="#172842", activecolor="#285c9f",
            bordercolor="#2b4566", borderwidth=1,
            x=0, y=1.45, xanchor="left", yanchor="top",
            font=dict(size=11),
        ),
    )
    y_min = min(counts)
    y_max = max(counts)
    y_padding = max((y_max - y_min) * 0.18, counts[-1] * 0.002, 1)
    figure.update_yaxes(
        row=1, col=1, range=[y_min - y_padding, y_max + y_padding],
        showgrid=True, gridcolor="#22344d", griddash="dot",
        tickfont=dict(color="#95afd0"), separatethousands=True,
        zeroline=False, title_text="SUBSKRYBENCI", title_font=dict(color="#8ca8c6", size=10),
    )
    figure.update_yaxes(
        row=2, col=1, showgrid=True, gridcolor="#203249", griddash="dot",
        zeroline=True, zerolinecolor="#556981",
        tickfont=dict(color="#95afd0"),
        title_text="ZMIANA", title_font=dict(color="#8ca8c6", size=10),
    )
    figure.update_layout(
        updatemenus=[dict(
            type="buttons", direction="right", x=1, xanchor="right", y=1.19,
            bgcolor="#172842", bordercolor="#2b4566", font=dict(color="#d5e9ff", size=11),
            showactive=True, active=0,
            buttons=[
                dict(label="Wartości: WŁ.", method="restyle",
                     args=[{"mode": "lines+markers+text"}, [0]]),
                dict(label="Wartości: WYŁ.", method="restyle",
                     args=[{"mode": "lines+markers"}, [0]]),
            ],
        )],
    )

    plot_html = figure.to_html(
        full_html=False, include_plotlyjs=True,
        config={"displaylogo": False,
                "modeBarButtonsToRemove": ["lasso2d", "select2d"],
                "responsive": True,
                "locale": "pl"},
    )

    # Pełna dzienna tabela pomaga odczytać każdą wartość bez przybliżania wykresu.
    sample_rows = []
    for i in range(len(samples) - 1, -1, -1):
        s = samples[i]
        d = datetime.fromisoformat(s["local_date"]).strftime("%d.%m.%Y")
        change = deltas[i]
        delta_txt = "—" if change is None else (f"{change:+,}".replace(",", " "))
        delta_cls = "positive" if change is not None and change > 0 else ("negative" if change is not None and change < 0 else "neutral")
        no_of_videos = len(grouped.get(s["local_date"], []))
        pub_txt = (f"<span class='video-dot'>▶</span> {no_of_videos} film" +
                   ("y" if no_of_videos > 1 else "")) if no_of_videos else "—"
        sample_rows.append(
            f"<tr><td>{d}</td><td class='number'>{spaced(s['subscriber_count'])}</td>"
            f"<td class='{delta_cls} number'>{delta_txt}</td><td>{pub_txt}</td></tr>"
        )

    # Widok publikacji obejmuje także filmy pobrane sprzed pierwszego pomiaru.
    publication_rows = []
    for v in sorted(videos, key=lambda value: value["published_at_utc"], reverse=True):
        pub = datetime.fromisoformat(v["published_at_utc"]).astimezone(TZ)
        url = "https://www.youtube.com/watch?v=" + v["video_id"]
        publication_rows.append(
            f"<tr><td>{pub:%d.%m.%Y %H:%M}</td>"
            f"<td><a target='_blank' rel='noopener noreferrer' href='{html.escape(url, quote=True)}'>"
            f"{html.escape(v['title'])} <span aria-hidden='true'>↗</span></a></td></tr>"
        )

    last = datetime.fromisoformat(samples[-1]["checked_at_utc"]).astimezone(TZ)
    title = html.escape(channel["title"])
    channel_url = html.escape(channel["channel_url"], quote=True)
    demo_alert = ("<div class='demo'>DANE DEMONSTRACYJNE — poniższe wartości są fikcyjne.</div>"
                  if demo else "")
    growth_class = "positive" if growth > 0 else "negative" if growth < 0 else "neutral"

    # Używamy szablonu zamiast f-stringa, aby nie kolidować z nawiasami CSS/JS.
    page = r"""<!doctype html>
<html lang="pl"><head><meta charset="utf-8"><meta name="viewport" content="width=device-width,initial-scale=1">
<title>BTC Audience Monitor · __PAGE_TITLE__</title>
<style>
:root{color-scheme:dark;--bg:#090f1c;--card:#111c30;--line:#23354f;--text:#e5efff;--muted:#91a6c4;--blue:#4c9fff;--btc:#f6ac42}
*{box-sizing:border-box}html{scroll-behavior:smooth}body{margin:0;color:var(--text);font-family:Inter,Segoe UI,Arial,sans-serif;background:radial-gradient(ellipse at 90% 0%,#102847 0%,transparent 38%),radial-gradient(ellipse at 0% 40%,#141e35 0%,transparent 48%),var(--bg);min-height:100vh}
a{color:#73b5ff;text-decoration:none}a:hover{color:#b7dcff;text-decoration:underline}
.shell{max-width:1310px;margin:0 auto;padding:30px 25px 64px;position:relative}
.topline{display:flex;align-items:center;justify-content:space-between;flex-wrap:wrap;gap:20px;padding-bottom:24px;border-bottom:1px solid #27344a}
.brand{display:flex;gap:15px;align-items:center}.btc-logo{display:grid;place-items:center;width:54px;height:54px;border-radius:15px;background:linear-gradient(140deg,#f8bc55,#df8732);box-shadow:0 6px 30px #fbb34e20;color:#152035;font-weight:800;font-size:36px;line-height:1}
.brand-tag{font-size:12px;letter-spacing:2.2px;font-weight:700;color:var(--btc)}.brand-name{font-size:20px;letter-spacing:.1px;font-weight:760;margin-top:4px}
.pill{display:inline-flex;align-items:center;gap:8px;font-size:12px;border:1px solid #345070;background:#142238;padding:10px 15px;border-radius:40px;color:#a6c9ed}
.pill:before{content:"";width:7px;height:7px;background:#37d6ad;border-radius:50%;box-shadow:0 0 9px #37d6ad60}
.title-area{margin:28px 0 24px;display:flex;justify-content:space-between;align-items:end;flex-wrap:wrap;gap:16px}
.eyebrow{font-size:11px;color:#e0a653;letter-spacing:2px;font-weight:800;text-transform:uppercase;margin:0 0 9px}
h1{font-size:clamp(24px,3vw,36px);font-weight:760;letter-spacing:-.8px;margin:0 0 8px}
.subheading{font-size:14px;line-height:1.5;color:var(--muted);margin:0}.channel-link{border:1px solid #365573;border-radius:10px;padding:11px 16px;font-size:13px;white-space:nowrap}
.kpis{display:grid;grid-template-columns:repeat(4,minmax(0,1fr));gap:14px;margin:0 0 18px}
.kpi{border:1px solid var(--line);background:linear-gradient(140deg,#162641,#101c30);border-radius:15px;padding:20px 22px;min-width:0;position:relative;overflow:hidden}
.kpi:after{content:"";position:absolute;right:-35px;top:-35px;width:80px;height:80px;border:1px solid #304b6c;border-radius:50%;opacity:.5}
.kpi-label{font-size:12px;line-height:1.5;color:#9aafc9;margin-bottom:10px}.kpi-value{font-weight:780;font-size:clamp(21px,2.3vw,31px);white-space:nowrap;letter-spacing:-.7px}
.kpi-note{color:#7896bb;font-size:11px;margin-top:10px}.blue{color:#75b8ff}.positive{color:#44d1b0!important}.negative{color:#ff7687!important}.neutral{color:#b8cce3!important}
.panel{background:linear-gradient(160deg,#131f34,#101a2b);border:1px solid var(--line);border-radius:18px;padding:21px 24px;margin-top:17px;box-shadow:0 15px 40px #00000021}
.panel-head{display:flex;flex-wrap:wrap;justify-content:space-between;align-items:center;gap:12px}
h2{font-size:17px;letter-spacing:-.15px;margin:4px 0 10px}.desc{font-size:12px;line-height:1.6;color:var(--muted);margin:6px 0}
.legend{display:flex;flex-wrap:wrap;gap:18px;font-size:12px;color:#c3d4e9}.legend-item{display:inline-flex;gap:8px;align-items:center}
.legend-blue{height:3px;width:25px;background:var(--blue);border-radius:4px}.legend-gold{height:0;width:25px;border-top:2px dashed var(--btc)}
.plot-shell{margin-top:12px;min-width:0;width:100%;overflow:hidden}.plot-shell .plotly-graph-div{width:100%!important}
.hint{padding:13px 16px;border-radius:11px;background:#0d1b2e;border:1px solid #273b56;font-size:12px;line-height:1.65;color:#a5bddc;margin-top:0}
.hint b{color:#dbe9fb}.section-tag{color:#e0a653;font-size:10px;letter-spacing:1.6px;font-weight:750;text-transform:uppercase;margin:0 0 8px}
.scroll{max-height:380px;overflow:auto;scrollbar-color:#365371 #0e1b2d}table{width:100%;border-collapse:collapse;font-size:13px}
th,td{padding:13px 10px;border-bottom:1px solid #24364e;text-align:left;vertical-align:middle}
th{font-weight:700;letter-spacing:.8px;font-size:10px;color:#94afcd;text-transform:uppercase;background:#14243a;position:sticky;top:0;z-index:2}
td{color:#bdcee6}td:first-child{white-space:nowrap}td.number{font-weight:700;font-variant-numeric:tabular-nums}tbody tr:hover{background:#1a2b44}
.video-dot{color:var(--btc);font-size:12px}.two-col{display:grid;grid-template-columns:minmax(0,1.05fr) minmax(0,.95fr);gap:17px}
.two-col .panel{min-width:0}.video-table td:first-child{font-size:11px;color:#94afcc}.video-table td a{overflow-wrap:anywhere}
footer{font-size:11px;color:#7892b3;line-height:1.8;padding:25px 1px 0}.demo{padding:13px;background:#3e3019;border:1px solid #9e7729;border-radius:10px;color:#ffdc98;margin-bottom:17px;font-weight:700}
@media(max-width:1050px){.kpis{grid-template-columns:repeat(2,minmax(0,1fr))}.two-col{grid-template-columns:1fr}}
@media(max-width:620px){.shell{padding:18px 12px 42px}.topline{gap:13px}.panel{padding:16px 10px}.plot-shell{margin-left:-3px}.kpis{gap:9px}.kpi{padding:15px 11px}.kpi-value{font-size:21px}.brand-name{font-size:16px}.brand-tag{letter-spacing:1.1px}.btc-logo{width:45px;height:45px;font-size:30px}.title-area{margin:20px 0}th,td{padding:11px 7px;font-size:12px}}
</style></head><body><main class="shell">
<header class="topline"><div class="brand"><div class="btc-logo" aria-hidden="true">₿</div>
<div><div class="brand-tag">BITCOIN · AUDIENCE INTELLIGENCE</div><div class="brand-name">BTC Audience Monitor</div></div></div>
<div class="pill">Ostatnia aktualizacja: __UPDATED__ · PL</div></header>
<div class="title-area"><div><p class="eyebrow">YouTube / obserwacja zainteresowania</p>
<h1>__PAGE_TITLE__</h1><p class="subheading">Śledzenie liczby subskrybentów i dat publikacji filmów na jednej osi czasu.</p></div>
<a class="channel-link" href="__CHANNEL_URL__" target="_blank" rel="noopener noreferrer">Otwórz kanał YouTube ↗</a></div>
__DEMO_ALERT__
<section class="kpis">
<div class="kpi"><div class="kpi-label">Aktualnie widoczni subskrybenci</div><div class="kpi-value blue">__SUBSCRIBERS__</div><div class="kpi-note">Publiczna liczba z YouTube</div></div>
<div class="kpi"><div class="kpi-label">Zmiana od pierwszego odczytu</div><div class="kpi-value __GROWTH_CLASS__">__GROWTH__</div><div class="kpi-note">Między pierwszym i ostatnim pomiarem</div></div>
<div class="kpi"><div class="kpi-label">Zapisanych pomiarów</div><div class="kpi-value">__SAMPLES__</div><div class="kpi-note">Zarejestrowane dni</div></div>
<div class="kpi"><div class="kpi-label">Długość obserwacji</div><div class="kpi-value">__DAYS__ <span style="font-size:14px;color:#91a6c4">dni</span></div><div class="kpi-note">Od pierwszego zapisu</div></div>
</section>
<section class="panel"><div class="panel-head"><div><p class="section-tag">01 / ANALIZA</p><h2>Subskrybenci vs. publikacje filmów</h2></div>
<div class="legend"><span class="legend-item"><span class="legend-blue"></span> Dzienny pomiar</span>
<span class="legend-item"><span class="legend-gold"></span> Premiera filmu</span></div></div>
<div class="plot-shell">__PLOT_HTML__</div>
<div class="hint"><b>Jak czytać wykres?</b> Niebieskie punkty i liczby oznaczają rzeczywiste dzienne odczyty. <b>Złote pionowe przerywane linie</b> pokazują daty opublikowania filmów; najedź na złoty trójkąt, aby zobaczyć tytuły. Domyślnie widzisz ostatnie 14 dni; możesz wybrać 30 / 90 dni lub całą historię. Jeśli etykiety na wykresie się nakładają, użyj opcji „Wartości: WYŁ.” lub przybliż wykres.</div>
</section>
<div class="two-col">
<section class="panel"><p class="section-tag">02 / DANE</p><h2>Dzienne odczyty subskrybentów</h2>
<p class="desc">Pełna historia dzień po dniu, najnowsze wartości na górze.</p><div class="scroll"><table><thead><tr><th>Data</th><th>Subskrybenci</th><th>Zmiana</th><th>Filmy</th></tr></thead><tbody>__SAMPLE_ROWS__</tbody></table></div></section>
<section class="panel"><p class="section-tag">03 / PUBLIKACJE</p><h2>Opublikowane filmy</h2>
<p class="desc">Godziny według czasu polskiego. Kliknij tytuł, aby otworzyć film.</p><div class="scroll"><table class="video-table"><thead><tr><th>Data</th><th>Film</th></tr></thead><tbody>__PUBLICATION_ROWS__</tbody></table></div></section>
</div>
<footer>Informacja: YouTube publicznie zaokrągla liczbę subskrybentów do 3 cyfr znaczących. Pominięte dni nie są sztucznie uzupełniane. Zmiana obserwowana po publikacji nie dowodzi, że spowodował ją dany film. Źródło: YouTube Data API v3 · Lokalna baza SQLite.</footer>
</main></body></html>"""
    replacements = {
        "__PAGE_TITLE__": title,
        "__CHANNEL_URL__": channel_url,
        "__UPDATED__": last.strftime("%d.%m.%Y %H:%M"),
        "__DEMO_ALERT__": demo_alert,
        "__SUBSCRIBERS__": spaced(counts[-1]),
        "__GROWTH_CLASS__": growth_class,
        "__GROWTH__": f"{growth:+,}".replace(",", " "),
        "__SAMPLES__": str(len(samples)),
        "__DAYS__": str(days_since_start),
        "__PLOT_HTML__": plot_html,
        "__SAMPLE_ROWS__": "".join(sample_rows),
        "__PUBLICATION_ROWS__": "".join(publication_rows) if publication_rows else "<tr><td colspan='2'>Brak pobranych publikacji.</td></tr>",
    }
    for token, value in replacements.items():
        page = page.replace(token, value)

    html_path.parent.mkdir(parents=True, exist_ok=True)
    html_path.write_text(page, encoding="utf-8")
    with csv_path.open("w", newline="", encoding="utf-8-sig") as out:
        writer = csv.writer(out, delimiter=";")
        writer.writerow(["Data odczytu (PL)", "Timestamp UTC", "Subskrybenci (zaokrąglone)",
                         "Zmiana od poprzedniego odczytu", "Wyświetlenia", "Liczba filmów"])
        writer.writerows(exported_rows(samples))
    print(f"Wygenerowano: {html_path} i {csv_path}")


def make_demo(path: Path) -> str:
    import random
    rnd = random.Random(20261019)
    channel_id = "UC_DEMO_MONITOR"
    today = datetime.now(TZ).date()
    with closing(database(path)) as conn, conn:
        conn.execute("DELETE FROM videos WHERE channel_id=?", (channel_id,))
        conn.execute("DELETE FROM snapshots WHERE channel_id=?", (channel_id,))
        conn.execute("INSERT INTO channels VALUES(?,?,?) ON CONFLICT(channel_id) DO UPDATE SET title=excluded.title",
                     (channel_id, "PRZYKŁADOWY KANAŁ — DANE DEMO", "https://www.youtube.com/"))
        count = 24500
        video_days = {3, 9, 16, 24, 33, 40}
        for i in range(45):
            day = today - timedelta(days=44 - i)
            count += rnd.randrange(-20, 65) + (170 if i in video_days else 0)
            # Public counters are rounded to 3 significant figures.
            count = (count // 100) * 100
            ts = datetime.combine(day, datetime.min.time(), tzinfo=TZ).replace(hour=13).astimezone(timezone.utc)
            conn.execute("INSERT INTO snapshots VALUES(?,?,?,?,?,?)", (channel_id, day.isoformat(),
                         ts.isoformat(), count, None, None))
            if i in video_days:
                conn.execute("INSERT INTO videos VALUES(?,?,?,?)", (f"demo_{i}", channel_id,
                             f"Przykładowy film numer {len([d for d in video_days if d<=i])}",
                             ts.isoformat()))
    return channel_id


def main() -> None:
    parser = argparse.ArgumentParser(description="Dzienny monitor kanału YouTube + wykres i eksport CSV")
    action = parser.add_mutually_exclusive_group()
    action.add_argument("--collect", action="store_true", help="Pobierz dane z YouTube i odśwież wykres (domyślne)")
    action.add_argument("--render", action="store_true", help="Odśwież wykres wyłącznie z lokalnych danych")
    action.add_argument("--demo", action="store_true", help="Wygeneruj demonstrację na fikcyjnych danych")
    args = parser.parse_args()
    try:
        if args.demo:
            path = DATA_DIR / "demo.sqlite"
            channel_id = make_demo(path)
            render(db_path=path, html_path=ROOT / "demo.html", csv_path=ROOT / "demo.csv",
                   demo=True, channel_id=channel_id)
        elif args.render:
            render()
        else:
            channel_id = collect()
            render(channel_id=channel_id)
    except (OSError, RuntimeError, ValueError, sqlite3.Error) as exc:
        print("BŁĄD:", exc, file=sys.stderr)
        sys.exit(1)


if __name__ == "__main__":
    main()
