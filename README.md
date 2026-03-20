# Call intake performance report

**Live site:** static files at repo root — `index.html` + `charts/` (no raw call logs in git).

## Vercel + GitHub

1. Import the repo on [Vercel](https://vercel.com/new).
2. **Root Directory:** leave as **`./`** (default).
3. **Framework Preset:** **Other** (or “Other” / static — not Python).
4. **Build / Install:** leave **empty** (optional: Vercel reads `vercel.json`).

Redeploy after each push to `main`.

## Regenerate charts locally

```bash
cd scripts
pip install -r requirements.txt
python build_intake_report.py
```

Update the Excel path inside `scripts/build_intake_report.py` if needed. Then copy:

- `report_output/VIEW_REPORT.html` → `index.html` (repo root)
- `report_output/charts/*.png` → `charts/`

Commit and push to update the live site.

## February vs March month-over-month report

Paths are set inside `scripts/build_mom_report.py` (February export + March-through-20 export). Regenerate:

```bash
python scripts/build_mom_report.py
```

This overwrites **`index.html`** at the repo root and writes `report_output/MOM_REPORT_FEB_MAR_2026.html`.

### One-command preview (no GitHub)

From repo root:

```bash
npx vercel
```
