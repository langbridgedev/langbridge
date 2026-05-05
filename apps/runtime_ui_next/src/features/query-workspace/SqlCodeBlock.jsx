import { useMemo, useRef, useState } from "react";

const SQL_KEYWORDS = new Set([
  "select",
  "from",
  "where",
  "join",
  "left",
  "right",
  "inner",
  "outer",
  "full",
  "on",
  "group",
  "by",
  "order",
  "having",
  "limit",
  "offset",
  "as",
  "and",
  "or",
  "not",
  "in",
  "is",
  "null",
  "case",
  "when",
  "then",
  "else",
  "end",
  "with",
  "union",
  "all",
  "distinct",
  "desc",
  "asc",
  "between",
  "like",
]);

const SQL_FUNCTIONS = new Set([
  "avg",
  "cast",
  "coalesce",
  "count",
  "date",
  "date_trunc",
  "max",
  "min",
  "round",
  "sum",
]);

const TOKEN_PATTERN =
  /(--[^\n]*|\/\*[\s\S]*?\*\/|'(?:''|[^'])*'|"(?:\\"|[^"])*"|\b[A-Za-z_][A-Za-z0-9_]*\b|\b\d+(?:\.\d+)?\b)/g;

export function SqlCodeBlock({ sql, label = "SQL query", compact = false }) {
  return (
    <pre className={compact ? "sql-code sql-code--compact" : "sql-code"} aria-label={label}>
      <code>{renderSqlTokens(sql)}</code>
    </pre>
  );
}

export function SqlEditor({
  value,
  onChange,
  disabled = false,
  placeholder = "Write SQL...",
  suggestions = [],
}) {
  const highlightRef = useRef(null);
  const textareaRef = useRef(null);
  const [caretIndex, setCaretIndex] = useState(0);
  const [focused, setFocused] = useState(false);
  const [manualSuggest, setManualSuggest] = useState(false);
  const currentToken = readCurrentToken(value, caretIndex);
  const visibleSuggestions = useMemo(
    () => filterSuggestions(suggestions, currentToken).slice(0, 8),
    [suggestions, currentToken],
  );
  const shouldShowSuggestions =
    focused &&
    !disabled &&
    visibleSuggestions.length > 0 &&
    (manualSuggest || currentToken.length >= 2);

  function handleScroll(event) {
    if (!highlightRef.current) {
      return;
    }
    highlightRef.current.scrollTop = event.currentTarget.scrollTop;
    highlightRef.current.scrollLeft = event.currentTarget.scrollLeft;
  }

  function updateCaret(target) {
    setCaretIndex(Number(target?.selectionStart || 0));
  }

  function applySuggestion(suggestion) {
    const textarea = textareaRef.current;
    const insertText = String(suggestion?.insertText || suggestion?.label || "").trim();
    if (!insertText || !textarea) {
      return;
    }
    const range = tokenRangeAt(value, textarea.selectionStart || 0);
    const nextValue = `${value.slice(0, range.start)}${insertText}${value.slice(range.end)}`;
    const nextCaret = range.start + insertText.length;
    onChange(nextValue);
    setManualSuggest(false);
    window.requestAnimationFrame(() => {
      textarea.focus();
      textarea.setSelectionRange(nextCaret, nextCaret);
      setCaretIndex(nextCaret);
    });
  }

  function handleKeyDown(event) {
    if ((event.ctrlKey || event.metaKey) && event.key === " ") {
      event.preventDefault();
      setManualSuggest(true);
      return;
    }
    if (event.key === "Escape") {
      setManualSuggest(false);
      return;
    }
    if (event.key === "Tab" && shouldShowSuggestions && visibleSuggestions[0]) {
      event.preventDefault();
      applySuggestion(visibleSuggestions[0]);
    }
  }

  return (
    <div className="sql-editor-shell">
      <pre ref={highlightRef} className="sql-editor-highlight" aria-hidden="true">
        <code>{renderSqlTokens(value || "\n")}</code>
      </pre>
      <textarea
        ref={textareaRef}
        className="sql-editor-input"
        value={value}
        disabled={disabled}
        placeholder={placeholder}
        spellCheck="false"
        onBlur={() => window.setTimeout(() => setFocused(false), 120)}
        onChange={(event) => {
          onChange(event.target.value);
          updateCaret(event.target);
        }}
        onClick={(event) => updateCaret(event.target)}
        onFocus={(event) => {
          setFocused(true);
          updateCaret(event.target);
        }}
        onKeyDown={handleKeyDown}
        onKeyUp={(event) => updateCaret(event.target)}
        onScroll={handleScroll}
      />
      {shouldShowSuggestions ? (
        <div className="sql-suggestion-bar">
          <span>Suggestions</span>
          <div>
            {visibleSuggestions.map((suggestion) => (
              <button
                type="button"
                key={`${suggestion.kind || "item"}-${suggestion.insertText || suggestion.label}`}
                onMouseDown={(event) => {
                  event.preventDefault();
                  applySuggestion(suggestion);
                }}
              >
                <strong>{suggestion.label}</strong>
                {suggestion.kind ? <small>{suggestion.kind}</small> : null}
              </button>
            ))}
          </div>
        </div>
      ) : null}
    </div>
  );
}

function renderSqlTokens(sql) {
  const source = String(sql || "");
  const tokens = [];
  let cursor = 0;
  let match = TOKEN_PATTERN.exec(source);

  while (match) {
    if (match.index > cursor) {
      tokens.push(source.slice(cursor, match.index));
    }

    const token = match[0];
    tokens.push(
      <span key={`${match.index}-${token}`} className={classNameForToken(token)}>
        {token}
      </span>,
    );
    cursor = match.index + token.length;
    match = TOKEN_PATTERN.exec(source);
  }

  if (cursor < source.length) {
    tokens.push(source.slice(cursor));
  }

  TOKEN_PATTERN.lastIndex = 0;
  return tokens.length > 0 ? tokens : "\n";
}

function classNameForToken(token) {
  const lower = String(token || "").toLowerCase();
  if (lower.startsWith("--") || lower.startsWith("/*")) {
    return "sql-comment";
  }
  if (lower.startsWith("'") || lower.startsWith('"')) {
    return "sql-string";
  }
  if (/^\d/.test(lower)) {
    return "sql-number";
  }
  if (SQL_KEYWORDS.has(lower)) {
    return "sql-keyword";
  }
  if (SQL_FUNCTIONS.has(lower)) {
    return "sql-function";
  }
  return "sql-identifier";
}

function filterSuggestions(suggestions, token) {
  const normalizedToken = String(token || "").trim().toLowerCase();
  const items = Array.isArray(suggestions) ? suggestions : [];
  if (!normalizedToken) {
    return items;
  }
  const startsWith = [];
  const includes = [];
  items.forEach((item) => {
    const label = String(item?.label || item?.insertText || "").toLowerCase();
    const insertText = String(item?.insertText || "").toLowerCase();
    if (label.startsWith(normalizedToken) || insertText.startsWith(normalizedToken)) {
      startsWith.push(item);
    } else if (label.includes(normalizedToken) || insertText.includes(normalizedToken)) {
      includes.push(item);
    }
  });
  return [...startsWith, ...includes];
}

function readCurrentToken(value, caretIndex) {
  const range = tokenRangeAt(value, caretIndex);
  return String(value || "").slice(range.start, range.end);
}

function tokenRangeAt(value, caretIndex) {
  const source = String(value || "");
  const index = Math.max(0, Math.min(Number(caretIndex || 0), source.length));
  let start = index;
  let end = index;
  while (start > 0 && /[A-Za-z0-9_$.]/.test(source[start - 1])) {
    start -= 1;
  }
  while (end < source.length && /[A-Za-z0-9_$.]/.test(source[end])) {
    end += 1;
  }
  return { start, end };
}
