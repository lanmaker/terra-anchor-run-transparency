from pathlib import Path

import pandas as pd


def write_threeparttable(
    df: pd.DataFrame,
    path: Path,
    notes: str | None = None,
    column_format: str | None = None,
    float_format: str = "%.4f",
    index: bool = False,
) -> None:
    table = df.to_latex(
        index=index,
        escape=False,
        float_format=float_format,
        column_format=column_format,
    ).strip()

    parts = ["\\begin{threeparttable}", table]
    if notes:
        parts.extend(
            [
                "\\begin{tablenotes}[flushleft]",
                "\\footnotesize",
                f"\\item \\textit{{Notes:}} {notes}",
                "\\end{tablenotes}",
            ]
        )
    parts.append("\\end{threeparttable}")

    Path(path).write_text("\n".join(parts) + "\n")
