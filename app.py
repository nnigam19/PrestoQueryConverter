# app.py
import io
import os
import zipfile
from pathlib import Path
import streamlit as st
from converter import (
    convert_blob,
    safe_split_sql,
    is_semantically_same,
    normalize_identifiers,
    quoted_identifier_set,
)

st.set_page_config(page_title="Presto → Databricks SQL Converter", page_icon="🧰", layout="wide")
st.title("Presto → Databricks SQL Converter")
st.caption("Accelerated Conversion to DBSQL")

tab1, tab2 = st.tabs(["Convert text", "Batch convert file"])


# ----------------------------------------------------------------------
# Unified converter for Streamlit (classification: converted / compatible / errors)
# ----------------------------------------------------------------------
def convert_full(sql_text: str):
    tokens = safe_split_sql(sql_text)

    converted_arr = []
    compatible_arr = []
    errors_arr = []

    for idx, t in enumerate(tokens, start=1):
        conv, err = convert_blob(t)

        if err:
            errors_arr.append(f"-- QUERY {idx}\n-- ERROR:\n{err}\n")
            continue

        # Determine classification: Compatible vs Converted
        orig_ast = normalize_identifiers(t).strip().rstrip(";")
        conv_ast = conv.strip().rstrip(";")

        same_ast = is_semantically_same(orig_ast, conv_ast)
        orig_q = quoted_identifier_set(t)
        conv_q = quoted_identifier_set(conv)

        if same_ast and orig_q == conv_q:
            # Already compatible
            compatible_arr.append(f"-- QUERY {idx}\n{t.strip()};\n")
        else:
            # Converted
            converted_arr.append(f"-- QUERY {idx}\n{conv.strip()};\n")

    return (
        "\n".join(converted_arr),
        "\n".join(errors_arr),
        "\n".join(compatible_arr),
    )


# ----------------------------------------------------------------------
# Render results + download buttons
# ----------------------------------------------------------------------
def render_results(converted, errors, compatible):
    st.subheader("Results")

    with st.expander("✔️ Converted SQL"):
        st.code(converted or "-- No converted queries --", language="sql")

    with st.expander("⚠️ Already Compatible SQL"):
        st.code(compatible or "-- None --", language="sql")

    with st.expander("❌ Errors"):
        st.code(errors or "-- No errors --", language="sql")

    # Downloads
    st.subheader("Download Output Files")
    c1, c2, c3 = st.columns(3)

    with c1:
        st.download_button(
            "Download converted.sql",
            data=converted.encode("utf-8"),
            file_name="converted.sql",
            mime="text/sql",
        )

    with c2:
        st.download_button(
            "Download compatible.sql",
            data=compatible.encode("utf-8"),
            file_name="compatible.sql",
            mime="text/sql",
        )

    with c3:
        st.download_button(
            "Download errors.sql",
            data=errors.encode("utf-8"),
            file_name="errors.sql",
            mime="text/sql",
        )


# ----------------------------------------------------------------------
# MAIN Logic used by both tabs
# ----------------------------------------------------------------------
def convert_and_render(sql_text: str):
    if not sql_text or not sql_text.strip():
        st.info("Paste Presto SQL to convert.")
        return

    with st.spinner("Converting…"):
        converted, errors, compatible = convert_full(sql_text)

    st.success("Conversion complete!")
    render_results(converted, errors, compatible)


# ----------------------------------------------------------------------
# TAB 1 – Convert raw text
# ----------------------------------------------------------------------
with tab1:
    st.subheader("Paste Presto SQL")
    input_sql = st.text_area(
        "Input (Presto SQL)",
        height=260,
        placeholder="SELECT * FROM some_table WHERE regexp_replace(col, '[^0-9]', '') = '123';"
    )

    if st.button("Convert"):
        convert_and_render(input_sql)


# ----------------------------------------------------------------------
# Helper functions for batch processing
# ----------------------------------------------------------------------
def extract_sql_files_from_zip(zip_file):
    """Extract all .sql/.txt files from a zip file."""
    files = []
    try:
        with zipfile.ZipFile(zip_file, 'r') as z:
            for file_info in z.filelist:
                if not file_info.is_dir() and (file_info.filename.endswith('.sql') or file_info.filename.endswith('.txt')):
                    content = z.read(file_info.filename).decode('utf-8', errors='ignore')
                    # Extract just the filename without path
                    filename = os.path.basename(file_info.filename)
                    files.append((filename, content))
    except Exception as e:
        st.error(f"Error extracting zip file: {e}")
    return files


def process_single_file(filename, content):
    """Process a single file and return results with proper naming."""
    base_name = os.path.splitext(filename)[0]
    
    converted, errors, compatible = convert_full(content)
    
    return {
        'converted': (f"{base_name}_converted.sql", converted),
        'compatible': (f"{base_name}_compatible.sql", compatible),
        'errors': (f"{base_name}_errors.sql", errors)
    }


def create_results_zip(all_results):
    """Create a zip file containing all result files."""
    zip_buffer = io.BytesIO()
    with zipfile.ZipFile(zip_buffer, 'w', zipfile.ZIP_DEFLATED) as zip_file:
        for results in all_results:
            for category in ['converted', 'compatible', 'errors']:
                filename, content = results[category]
                if content and content.strip():
                    zip_file.writestr(filename, content)
    zip_buffer.seek(0)
    return zip_buffer


# ----------------------------------------------------------------------
# TAB 2 – Batch convert uploaded SQL file(s) or zip
# ----------------------------------------------------------------------
with tab2:
    st.subheader("Upload SQL file(s) or zip")
    upl = st.file_uploader(
        "Choose file(s) or zip containing SQL files", 
        type=["sql", "txt", "zip"],
        accept_multiple_files=True
    )

    if upl:
        # Collect all files to process
        files_to_process = []
        
        for uploaded_file in upl:
            if uploaded_file.name.endswith('.zip'):
                # Extract SQL files from zip
                sql_files = extract_sql_files_from_zip(uploaded_file)
                files_to_process.extend(sql_files)
            else:
                # Regular SQL/TXT file
                try:
                    content = uploaded_file.getvalue().decode("utf-8", errors="ignore")
                    files_to_process.append((uploaded_file.name, content))
                except Exception as e:
                    st.error(f"Could not read {uploaded_file.name}: {e}")
        
        if files_to_process:
            st.info(f"Loaded {len(files_to_process)} file(s). Click Convert to process.")
            
            if st.button("Convert files", key="batch_convert"):
                all_results = []
                
                progress_bar = st.progress(0)
                status_text = st.empty()
                
                for idx, (filename, content) in enumerate(files_to_process):
                    status_text.text(f"Processing {idx + 1}/{len(files_to_process)}: {filename}")
                    
                    with st.spinner(f"Converting {filename}..."):
                        results = process_single_file(filename, content)
                        all_results.append(results)
                    
                    progress_bar.progress((idx + 1) / len(files_to_process))
                
                status_text.empty()
                progress_bar.empty()
                
                st.success(f"Processed {len(files_to_process)} file(s) successfully!")
                
                # Create and offer zip download
                results_zip = create_results_zip(all_results)
                
                st.subheader("Download Results")
                st.download_button(
                    "Download All Results (ZIP)",
                    data=results_zip,
                    file_name="conversion_results.zip",
                    mime="application/zip"
                )
                
                # Show summary
                with st.expander("Processing Summary"):
                    for idx, (filename, _) in enumerate(files_to_process):
                        st.write(f"**{idx + 1}. {filename}**")
                        results = all_results[idx]
                        
                        conv_file, conv_content = results['converted']
                        compat_file, compat_content = results['compatible']
                        err_file, err_content = results['errors']
                        
                        conv_count = len(safe_split_sql(conv_content)) if conv_content.strip() else 0
                        compat_count = len(safe_split_sql(compat_content)) if compat_content.strip() else 0
                        err_count = len(safe_split_sql(err_content)) if err_content.strip() else 0
                        
                        st.write(f"  - Converted: {conv_count} queries")
                        st.write(f"  - Compatible: {compat_count} queries")
                        st.write(f"  - Errors: {err_count} queries")