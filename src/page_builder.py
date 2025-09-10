from config import TZ, COUNTRY_CODE, DAYS_BACK, SITE_TITLE, SITE_TAGLINE, OUTPUT_DIR, OUTPUT_FILE


def build_page(fig_sections):
    # NOTE: We DO NOT include a <script src="plotly-latest..."> here.
    # The first figure's HTML will include the correct Plotly JS via include_plotlyjs="cdn".
    
    sections_html = "\n".join(
        f"""
        <section id="{sec['id']}">
          <h2>{sec['title']}</h2>
          <p class="blurb">{sec['blurb']}</p>
          {sec['fig_html']}
        </section>
        """ for sec in fig_sections
    )

    return f"""<!doctype html>
<html lang="en">
<head>
  <meta charset="utf-8">
  <title>{SITE_TITLE}</title>
  <meta name="viewport" content="width=device-width, initial-scale=1">
  <meta name="description" content="{SITE_TAGLINE}">
  <style>
    :root {{
      --maxw: 1100px; --pad: 16px; --accent: #0f766e; --text: #1f2937; --muted: #6b7280; --border: #e5e7eb;
    }}
    * {{ box-sizing: border-box; }}
    body {{ margin: 0; color: var(--text); font-family: system-ui, -apple-system, Segoe UI, Roboto, Inter, sans-serif; background: #fafafa; }}
    header {{ background: white; border-bottom: 1px solid var(--border); }}
    .wrap {{ max-width: var(--maxw); margin: 0 auto; padding: 18px var(--pad); }}
    h1 {{ margin: 0 0 6px; font-size: 1.6rem; }}
    .tagline {{ color: var(--muted); margin: 0; }}
    nav {{ margin-top: 10px; display: flex; gap: 12px; flex-wrap: wrap; }}
    nav a {{ text-decoration: none; color: var(--accent); border: 1px solid var(--border); padding: 6px 10px; border-radius: 999px; background: #fff; }}
    main .wrap {{ padding-top: 20px; }}
    section {{ background: white; border: 1px solid var(--border); border-radius: 14px; padding: 14px; margin-bottom: 22px; box-shadow: 0 1px 2px rgba(0,0,0,0.04); }}
    h2 {{ margin: 6px 0 10px; font-size: 1.25rem; }}
    .blurb {{ color: var(--muted); margin-top: 0; }}
    footer {{ color: var(--muted); font-size: 0.9rem; padding: 32px var(--pad); text-align: center; }}
  </style>
</head>
<body>
  <header>
    <div class="wrap">
      <h1>{SITE_TITLE}</h1>
      <p class="tagline">{SITE_TAGLINE}</p>
      <nav>
        <a href="#actual-load">Actual Load</a>
        <a href="#daily-avg">Daily Average</a>
        <a href="#notes">Notes</a>
      </nav>
    </div>
  </header>

  <main>
    <div class="wrap">
      {sections_html}
      <section id="notes">
        <h2>Notes</h2>
        <ul>
          <li>Times are displayed in {TZ}.</li>
          <li>A load for a specific hour indicates the average load during a one hour period. As an example, the data point at 15:00 indicates the load between 15:00-16:00.</li>
          <li>The daily average chart aggregates the hourly values by day.</li>
          <li>Data source: ENTSO-E (Actual Total Load for {COUNTRY_CODE}).</li>
        </ul>
      </section>
    </div>
  </main>

  <footer>
    Built with Python + Plotly, deployed on GitHub Pages.
  </footer>
</body>
</html>"""
