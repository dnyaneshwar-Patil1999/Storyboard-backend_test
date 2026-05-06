"""
preprocessing_integration.diff
==============================

Integration patch for core/preprocessing.py.

This file shows the EXACT changes needed inside _process_traditional_pdf()
to call pdf_input_cleaner BEFORE the existing per-page LLM heading detection
and element-extraction logic. Apply with: a careful read + manual edit.
Do NOT use `patch -p1` — these snippets are illustrative, not literal hunks.

The integration is conservative:
  • We DO NOT remove or replace the existing LLM-based heading detection
    (get_llm_page_by_page_structure_traditional). That path stays as a
    fallback for image-only/scanned PDFs.
  • We ADD the cleaner output as the PRIMARY heading source. If the cleaner
    detects >= 2 headings, we use those; otherwise we fall back to the LLM.
  • We ADD a block-level filter to extract_document_elements so that web
    cruft (share buttons, recurring headers, related-posts sections) is
    dropped before chunks are assembled.

Why the cleaner first, LLM second:
  • The LLM call costs ~1 GPT-4o-vision request per page. For the 9-page
    MBO PDF that's 9 calls, ~$0.30 and ~30 seconds.
  • The cleaner makes ZERO LLM calls and detects all 7 numbered topic
    headings on the MBO PDF correctly (verified by test_pdf_input_cleaner.py).
  • For documents the cleaner can't handle (no font-size signal, e.g.
    scanned PDFs), the LLM path still runs.
"""

# ──────────────────────────────────────────────────────────────────────────
# CHANGE 1 — top of preprocessing.py
# ──────────────────────────────────────────────────────────────────────────
"""
At the top of core/preprocessing.py, near the other imports:
"""
# fmt: off
INSERT_AT_TOP = '''
from pdf_input_cleaner import clean_pdf_blocks, detect_pdf_headings
'''


# ──────────────────────────────────────────────────────────────────────────
# CHANGE 2 — _process_traditional_pdf  (replace the heading-detection step)
# ──────────────────────────────────────────────────────────────────────────
"""
The existing function looks roughly like:

    def _process_traditional_pdf(doc, blob_name, oai_client, ...):
        has_toc, toc_entries = detect_toc_presence(doc, oai_client, config)
        llm_derived_outline = get_llm_page_by_page_structure_traditional(doc, oai_client, config)
        # ... position-tagging loop ...
        all_elements = extract_document_elements(doc, oai_client, config)
        contextualized_elements = assign_context_to_elements(all_elements, llm_derived_outline, ...)
        ...

Replace the heading-detection step (the call to
get_llm_page_by_page_structure_traditional) with a cleaner-first approach.
"""
REPLACE_HEADING_DETECTION = '''
def _process_traditional_pdf(doc, blob_name, oai_client, ..., text_analytics_client_instance):
    print(f"  - Processing '{blob_name}' as a TRADITIONAL DOCUMENT.")

    has_toc, toc_entries = detect_toc_presence(doc, oai_client, config)

    # ── NEW: Try fast cleaner-based heading detection first ──
    # Returns dicts: {page, pos, text, level} where level is one of
    # "Chapter", "Topic", "Subtopic". Uses font-size signals — no LLM call.
    cleaner_blocks   = clean_pdf_blocks(doc)            # filtered text blocks
    cleaner_headings = detect_pdf_headings(cleaner_blocks)  # heading dicts

    # Convert cleaner headings to the schema expected by
    # assign_context_to_elements: {page_num, pos, level, text}
    llm_derived_outline = []
    for h in cleaner_headings:
        llm_derived_outline.append({
            "page_num": h["page"],
            "pos":      h["pos"],
            "level":    h["level"],
            "text":     h["text"],
        })

    # Fallback: if cleaner found < 2 headings, the doc may be image-heavy or
    # scanned. Use the existing per-page LLM detection in that case.
    if len(llm_derived_outline) < 2:
        print(f"  - Cleaner found {len(llm_derived_outline)} headings; "
              f"falling back to per-page LLM detection.")
        llm_derived_outline = get_llm_page_by_page_structure_traditional(
            doc, oai_client, config)
    else:
        print(f"  - Cleaner found {len(llm_derived_outline)} headings; "
              f"skipping per-page LLM call.")

    # The position-tagging loop below is unchanged — it just makes sure the
    # outline entries have a y-position for sort-merging with elements.
    for p_num in range(len(doc)):
        page = doc.load_page(p_num)
        for block in page.get_text("blocks", sort=True):
            block_text = sanitize_text(block[4])
            if block_text:
                for heading_item in llm_derived_outline:
                    if (heading_item["page_num"] == p_num + 1
                            and sanitize_text(heading_item["text"]) in block_text):
                        heading_item["pos"] = block[1]
                        break

    llm_derived_outline.sort(key=lambda x: (x["page_num"], x.get("pos", 0)))

    # toc_chapter_map building — unchanged
    toc_chapter_map = {}
    if has_toc and toc_entries:
        for entry in toc_entries:
            if entry.get("level", "").lower() == "chapter":
                toc_chapter_map[entry["page"]] = entry["title"]
        toc_chapter_map = dict(sorted(toc_chapter_map.items()))

    # ── NEW: pass cleaner_blocks down to extract_document_elements ──
    # This lets that function skip the URL-bombs / share-button / related-posts
    # blocks rather than re-discovering them.
    all_elements = extract_document_elements(
        doc, oai_client, config,
        clean_blocks=cleaner_blocks,           # NEW kwarg
    )
    if not all_elements:
        print("    - No elements found in document.")
        return

    contextualized_elements = assign_context_to_elements(
        all_elements, llm_derived_outline, toc_chapter_map)
    logical_chunks = assemble_chunks_from_elements(
        contextualized_elements, oai_client, config)

    # ... rest of the function unchanged ...
'''


# ──────────────────────────────────────────────────────────────────────────
# CHANGE 3 — extract_document_elements  (accept clean_blocks kwarg)
# ──────────────────────────────────────────────────────────────────────────
"""
Add `clean_blocks=None` kwarg to extract_document_elements. When provided,
build a fast lookup set of (page, x0, y0) tuples and skip any page block
whose origin isn't in the set.
"""
EXTRACT_ELEMENTS_PATCH = '''
def extract_document_elements(doc, oai_client, config, clean_blocks=None):
    """Extract raw elements (text blocks, images, tables) from the document.

    If clean_blocks is provided (from pdf_input_cleaner.clean_pdf_blocks), only
    blocks whose (page, x0, y0) appears in clean_blocks are processed. This
    is how we skip share-button URL bombs and recurring headers/footers.
    """
    elements = []
    element_counter = 0

    # NEW: build keep-set if cleaner output was provided
    keep_set = None
    if clean_blocks is not None:
        keep_set = {
            (cb["page"], round(cb["x0"], 1), round(cb["y0"], 1))
            for cb in clean_blocks
        }

    graph_pages_from_detector = detect_graph_pages(doc, oai_client, config)
    p_fig = re.compile(r"^(figure|fig\\.?)\\s*...", re.IGNORECASE)
    p_tbl = re.compile(r"^(table|tab\\.?)\\s*...", re.IGNORECASE)

    for p_num in range(len(doc)):
        page = doc.load_page(p_num)
        for block in page.get_text("blocks", sort=True):
            x0, y0, x1, y1, text = block[:5]

            # NEW: skip blocks the cleaner already excluded
            if keep_set is not None:
                if (p_num + 1, round(x0, 1), round(y0, 1)) not in keep_set:
                    continue

            # ... rest of the existing per-block logic unchanged ...
            # (text classification, image extraction, table detection, etc.)
'''


# ──────────────────────────────────────────────────────────────────────────
# DEPLOYMENT SEQUENCE
# ──────────────────────────────────────────────────────────────────────────
"""
Stage 1 (zero-risk telemetry):
  • Drop pdf_input_cleaner.py into the azure_function-main/ directory.
  • Add the import to the top of preprocessing.py.
  • In _process_traditional_pdf, add ONLY this line near the start:
        cleaner_blocks = clean_pdf_blocks(doc)
        cleaner_headings = detect_pdf_headings(cleaner_blocks)
        logger.info(f"Cleaner would have produced "
                    f"{len(cleaner_blocks)} blocks, {len(cleaner_headings)} headings")
  • Don't actually USE the cleaner output yet. Just log it.
  • Deploy. Observe logs across a representative sample of production PDFs.
  • Confirm cleaner detects "reasonable" heading counts (2-30) on >80% of
    docs and doesn't over-prune (kept blocks > 10).

Stage 2 (heading replacement):
  • Apply CHANGE 2 above. Now the cleaner is the PRIMARY heading source;
    the LLM stays as the fallback for low-signal docs.
  • Deploy. Compare chunk counts and chapter/topic/subtopic accuracy
    between Stage 1 and Stage 2 on the same input set.

Stage 3 (block filtering):
  • Apply CHANGE 3 above. Now the cleaner also drops cruft blocks before
    they reach the chunk assembler.
  • This is the biggest behavioural change — verify against the 7 docs in
    the PDF Issues sheet of Review_Tracker.

Each stage is independently revertable.
"""
