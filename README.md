# Call intake performance report

Static site lives in **`web/`** (safe to deploy — no raw call logs).

## Deploy to Vercel (GitHub)

1. Create a new repo on GitHub (empty, no README).
2. From this folder:

   ```bash
   git init
   git add .gitignore README.md web/
   git commit -m "Add intake report static site"
   git branch -M main
   git remote add origin https://github.com/YOUR_USER/YOUR_REPO.git
   git push -u origin main
   ```

3. In [Vercel](https://vercel.com) → **Add New** → **Project** → import that repo.
4. **Important:** under **Configure Project**, set **Root Directory** to **`web`** (Folder: `web`). Leave build command empty. **Deploy**.

Your live URL will look like `https://your-project.vercel.app`.

### One-command preview (no GitHub)

From a terminal:

```bash
cd web
npx vercel
```

Follow the prompts; you’ll get a shareable `*.vercel.app` URL.

## Regenerate charts locally

Requires Python + the CallRail export path inside `build_intake_report.py`.

```bash
pip install -r requirements.txt
python build_intake_report.py
```

Then copy `report_output/VIEW_REPORT.html` → `web/index.html` and `report_output/charts/*.png` → `web/charts/`, commit, and push to update the live site.
