import json
import os
import tkinter as tk
from tkinter import messagebox, ttk

import pandas as pd

INPUT_PATH = os.path.join("0_needs_processing", "tickerlist.xlsx")
INPUT_SHEET = "Sheet1"
CONFIG_PATH = os.path.join("3_feature_and_variable_transformation", "model_feature_config.json")

METADATA_COLUMNS = {"ticker", "name", "country", "industry", "sector"}

DEFAULT_CONFIG = {
    "target_column": "returns_2025_ihs",
    "raw_target_column": "returns_2025",
    "raw_feature_columns": [],
    "transformed_feature_columns": [],
    "ihs_columns": [],
    "interactions": {},
    "pdp_2d_pairs": [],
}


def load_config() -> dict:
    if os.path.exists(CONFIG_PATH):
        with open(CONFIG_PATH, "r", encoding="utf-8") as f:
            cfg = json.load(f)
        merged = DEFAULT_CONFIG.copy()
        merged.update(cfg)
        return merged
    return DEFAULT_CONFIG.copy()


def load_columns() -> list[str]:
    if not os.path.exists(INPUT_PATH):
        raise FileNotFoundError(f"Input file not found: {INPUT_PATH}")
    df = pd.read_excel(INPUT_PATH, sheet_name=INPUT_SHEET, nrows=1)
    return [str(c) for c in df.columns]


def parse_lines(text: str) -> list[str]:
    return [line.strip() for line in text.splitlines() if line.strip()]


def parse_pairs(text: str) -> list[list[str]]:
    pairs = []
    for line in parse_lines(text):
        parts = [p.strip() for p in line.split(",", 1)]
        if len(parts) == 2 and parts[0] and parts[1]:
            pairs.append([parts[0], parts[1]])
    return pairs


def parse_interactions(text: str) -> dict[str, list[str]]:
    interactions = {}
    for line in parse_lines(text):
        if "=" not in line:
            continue
        name, pair_text = line.split("=", 1)
        parts = [p.strip() for p in pair_text.split(",", 1)]
        if name.strip() and len(parts) == 2 and parts[0] and parts[1]:
            interactions[name.strip()] = [parts[0], parts[1]]
    return interactions


def dedupe(items: list[str]) -> list[str]:
    out = []
    for item in items:
        if item and item not in out:
            out.append(item)
    return out


def main() -> None:
    cfg = load_config()
    columns = load_columns()
    feature_candidates = [c for c in columns if c not in METADATA_COLUMNS]

    os.makedirs(os.path.dirname(CONFIG_PATH), exist_ok=True)

    root = tk.Tk()
    root.title("Choose ML Features")
    root.geometry("980x760")
    root.minsize(820, 620)

    top_frame = ttk.Frame(root, padding=10)
    top_frame.pack(fill="x")

    ttk.Label(top_frame, text="Raw target column").grid(row=0, column=0, sticky="w")
    raw_target_var = tk.StringVar(value=cfg.get("raw_target_column", "returns_2025"))
    raw_target_combo = ttk.Combobox(top_frame, textvariable=raw_target_var, values=columns, state="readonly")
    raw_target_combo.grid(row=1, column=0, sticky="ew", padx=(0, 12))

    target_ihs_var = tk.BooleanVar(value=str(cfg.get("target_column", "")).endswith("_ihs"))
    ttk.Checkbutton(top_frame, text="Use IHS transformed target", variable=target_ihs_var).grid(
        row=1, column=1, sticky="w"
    )
    top_frame.columnconfigure(0, weight=1)

    notebook = ttk.Notebook(root)
    notebook.pack(fill="both", expand=True, padx=10, pady=(0, 10))

    feature_tab = ttk.Frame(notebook)
    advanced_tab = ttk.Frame(notebook)
    notebook.add(feature_tab, text="Features")
    notebook.add(advanced_tab, text="Interactions / PDP")

    toolbar = ttk.Frame(feature_tab, padding=(0, 0, 0, 8))
    toolbar.pack(fill="x")

    table_wrap = ttk.Frame(feature_tab)
    table_wrap.pack(fill="both", expand=True)

    canvas = tk.Canvas(table_wrap, highlightthickness=0)
    scrollbar = ttk.Scrollbar(table_wrap, orient="vertical", command=canvas.yview)
    rows_frame = ttk.Frame(canvas)
    rows_frame.bind("<Configure>", lambda _event: canvas.configure(scrollregion=canvas.bbox("all")))
    canvas.create_window((0, 0), window=rows_frame, anchor="nw")
    canvas.configure(yscrollcommand=scrollbar.set)
    canvas.pack(side="left", fill="both", expand=True)
    scrollbar.pack(side="right", fill="y")

    ttk.Label(rows_frame, text="Column").grid(row=0, column=0, sticky="w", padx=6, pady=4)
    ttk.Label(rows_frame, text="Raw feature").grid(row=0, column=1, sticky="w", padx=6, pady=4)
    ttk.Label(rows_frame, text="IHS feature").grid(row=0, column=2, sticky="w", padx=6, pady=4)

    raw_vars: dict[str, tk.BooleanVar] = {}
    ihs_vars: dict[str, tk.BooleanVar] = {}
    raw_selected = set(cfg.get("raw_feature_columns", []))
    ihs_selected = set(cfg.get("ihs_columns", []))
    transformed_selected = {
        c[:-4] for c in cfg.get("transformed_feature_columns", []) if c.endswith("_ihs")
    }

    for row_idx, col in enumerate(feature_candidates, start=1):
        raw_vars[col] = tk.BooleanVar(value=col in raw_selected)
        ihs_vars[col] = tk.BooleanVar(value=col in ihs_selected or col in transformed_selected)
        ttk.Label(rows_frame, text=col).grid(row=row_idx, column=0, sticky="w", padx=6, pady=2)
        ttk.Checkbutton(rows_frame, variable=raw_vars[col]).grid(row=row_idx, column=1, sticky="w", padx=6, pady=2)
        ttk.Checkbutton(rows_frame, variable=ihs_vars[col]).grid(row=row_idx, column=2, sticky="w", padx=6, pady=2)

    def set_all_raw(value: bool) -> None:
        for col, var in raw_vars.items():
            if col != raw_target_var.get():
                var.set(value)

    def set_all_ihs(value: bool) -> None:
        for col, var in ihs_vars.items():
            if col != raw_target_var.get():
                var.set(value)

    ttk.Button(toolbar, text="Select all raw", command=lambda: set_all_raw(True)).pack(side="left")
    ttk.Button(toolbar, text="Clear raw", command=lambda: set_all_raw(False)).pack(side="left", padx=(6, 0))
    ttk.Button(toolbar, text="Select all IHS", command=lambda: set_all_ihs(True)).pack(side="left", padx=(18, 0))
    ttk.Button(toolbar, text="Clear IHS", command=lambda: set_all_ihs(False)).pack(side="left", padx=(6, 0))

    ttk.Label(advanced_tab, text="Interactions, one per line: output_name=col_a,col_b").pack(
        anchor="w", padx=8, pady=(8, 2)
    )
    interactions_text = tk.Text(advanced_tab, height=12)
    interactions_text.pack(fill="both", expand=True, padx=8)
    interactions = cfg.get("interactions", {})
    interactions_text.insert("1.0", "\n".join([f"{name}={a},{b}" for name, (a, b) in interactions.items()]))

    ttk.Label(advanced_tab, text="PDP 2D pairs, one per line: col_a,col_b").pack(anchor="w", padx=8, pady=(8, 2))
    pairs_text = tk.Text(advanced_tab, height=8)
    pairs_text.pack(fill="both", expand=True, padx=8, pady=(0, 8))
    pairs_text.insert("1.0", "\n".join([f"{a},{b}" for a, b in cfg.get("pdp_2d_pairs", [])]))

    should_run_pipeline = {"value": False}

    def on_save() -> bool:
        raw_target = raw_target_var.get().strip()
        if not raw_target:
            messagebox.showerror("Validation Error", "Choose a raw target column.")
            return False

        raw_features = [col for col, var in raw_vars.items() if var.get() and col != raw_target]
        ihs_sources = [col for col, var in ihs_vars.items() if var.get() and col != raw_target]
        ihs_columns = dedupe(([raw_target] if target_ihs_var.get() else []) + ihs_sources)
        transformed_features = [f"{col}_ihs" for col in ihs_sources]
        target_column = f"{raw_target}_ihs" if target_ihs_var.get() else raw_target

        interactions = parse_interactions(interactions_text.get("1.0", "end"))
        if not raw_features and not transformed_features and not interactions:
            messagebox.showerror("Validation Error", "Select at least one feature.")
            return False

        data = {
            "target_column": target_column,
            "raw_target_column": raw_target,
            "ihs_columns": ihs_columns,
            "raw_feature_columns": raw_features,
            "transformed_feature_columns": transformed_features,
            "interactions": interactions,
            "pdp_2d_pairs": parse_pairs(pairs_text.get("1.0", "end")),
        }
        with open(CONFIG_PATH, "w", encoding="utf-8") as f:
            json.dump(data, f, indent=2)
        messagebox.showinfo("Saved", f"Saved config:\n{CONFIG_PATH}")
        return True

    def on_save_and_continue() -> None:
        if on_save():
            should_run_pipeline["value"] = True
            root.destroy()

    def on_cancel() -> None:
        should_run_pipeline["value"] = False
        root.destroy()

    bottom = ttk.Frame(root, padding=(10, 0, 10, 10))
    bottom.pack(fill="x")
    ttk.Button(bottom, text="Save config only", command=on_save).pack(side="left")
    run_button = ttk.Button(bottom, text="Save and run pipeline", command=on_save_and_continue)
    run_button.pack(side="left", padx=(8, 0))
    ttk.Button(bottom, text="Cancel pipeline", command=on_cancel).pack(side="right")
    run_button.focus_set()

    root.mainloop()
    if not should_run_pipeline["value"]:
        raise SystemExit(1)


if __name__ == "__main__":
    main()
