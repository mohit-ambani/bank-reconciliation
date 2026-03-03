import streamlit as st
import pandas as pd


def render_column_mapper(columns: list, required_fields: list) -> dict | None:
    """
    Render dropdown selectors for mapping bank statement columns to canonical fields.

    Args:
        columns: List of column names from the uploaded bank file.
        required_fields: List of canonical field names that must be mapped.

    Returns:
        Dict mapping canonical field name to selected column, or None if invalid.
    """
    st.subheader("Map Bank Statement Columns")
    st.caption("Select which column in your file corresponds to each required field.")

    column_map = {}
    cols = st.columns(len(required_fields))

    for i, field_name in enumerate(required_fields):
        with cols[i]:
            # Try to auto-detect a reasonable default
            default_idx = 0
            for j, col in enumerate(columns):
                if field_name.lower() in col.lower():
                    default_idx = j
                    break

            selected = st.selectbox(
                f"{field_name}",
                options=columns,
                index=default_idx,
                key=f"map_{field_name}",
            )
            column_map[field_name] = selected

    # Validate: no duplicate selections
    selected_values = list(column_map.values())
    if len(selected_values) != len(set(selected_values)):
        st.error("Each field must be mapped to a different column. Please fix duplicate selections.")
        return None

    return column_map


def render_dataframe_with_download(df: pd.DataFrame, label: str, key: str):
    """Display a DataFrame with row count header."""
    st.markdown(f"**{label}** ({len(df):,} rows)")
    if df.empty:
        st.info(f"No {label.lower()} records found.")
    else:
        st.dataframe(df, use_container_width=True, hide_index=True)


def render_metric_card(label: str, value, delta: str = None):
    """Render a Streamlit metric."""
    st.metric(label=label, value=value, delta=delta)
