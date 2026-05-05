"""Profile-level presentation guidance used by final response composition."""
from dataclasses import dataclass
import re
from typing import Any


_CURRENCY_MARKERS: tuple[tuple[str, str, tuple[str, ...]], ...] = (
    ("AUD", "A$", ("aud", "a$", "au$", "australian dollar", "australian dollars")),
    ("CAD", "C$", ("cad", "c$", "ca$", "canadian dollar", "canadian dollars")),
    ("NZD", "NZ$", ("nzd", "nz$", "new zealand dollar", "new zealand dollars")),
    ("SGD", "S$", ("sgd", "s$", "singapore dollar", "singapore dollars")),
    ("HKD", "HK$", ("hkd", "hk$", "hong kong dollar", "hong kong dollars")),
    ("GBP", "£", ("gbp", "£", "pound", "sterling")),
    ("USD", "$", ("usd", "$", "dollar")),
    ("EUR", "€", ("eur", "€", "euro")),
    ("CHF", "CHF", ("chf", "swiss franc", "swiss francs")),
    ("SEK", "kr", ("sek", "swedish krona", "swedish kronor")),
    ("NOK", "kr", ("nok", "norwegian krone", "norwegian kroner")),
    ("DKK", "kr", ("dkk", "danish krone", "danish kroner")),
    ("ISK", "kr", ("isk", "icelandic krona", "icelandic kronur")),
    ("PLN", "zł", ("pln", "zł", "zloty", "zlotys")),
    ("CZK", "Kč", ("czk", "kč", "czech koruna", "czech korunas")),
    ("HUF", "Ft", ("huf", "hungarian forint", "forint", "forints")),
    ("RON", "lei", ("ron", "romanian leu", "romanian lei")),
    ("BGN", "лв", ("bgn", "лв", "bulgarian lev", "bulgarian leva")),
    ("TRY", "₺", ("try", "₺", "turkish lira", "turkish lire")),
    ("JPY", "¥", ("jpy", "jp¥", "yen", "japanese yen")),
    ("CNY", "¥", ("cny", "cn¥", "rmb", "yuan", "renminbi", "chinese yuan")),
    ("KRW", "₩", ("krw", "₩", "won", "korean won", "south korean won")),
    ("INR", "₹", ("inr", "₹", "rupee", "rupees", "indian rupee", "indian rupees")),
    ("IDR", "Rp", ("idr", "rp", "rupiah", "indonesian rupiah")),
    ("MYR", "RM", ("myr", "rm", "ringgit", "malaysian ringgit")),
    ("PHP", "₱", ("php", "₱", "philippine peso", "philippine pesos")),
    ("THB", "฿", ("thb", "฿", "baht", "thai baht")),
    ("VND", "₫", ("vnd", "₫", "dong", "vietnamese dong")),
    ("AED", "د.إ", ("aed", "د.إ", "dirham", "dirhams", "uae dirham", "emirati dirham")),
    ("SAR", "﷼", ("sar", "ر.س", "saudi riyal", "saudi riyals")),
    ("QAR", "ر.ق", ("qar", "qatari riyal", "qatari riyals")),
    ("ILS", "₪", ("ils", "₪", "shekel", "shekels", "israeli shekel", "israeli new shekel")),
    ("ZAR", "R", ("zar", "rand", "south african rand")),
    ("BRL", "R$", ("brl", "r$", "real", "reais", "brazilian real")),
    ("MXN", "MX$", ("mxn", "mx$", "mexican peso", "mexican pesos")),
)

_MONETARY_TERMS = {
    "amount",
    "arr",
    "cac",
    "cash",
    "cost",
    "gmv",
    "gross",
    "margin",
    "mrr",
    "net",
    "pipeline",
    "price",
    "refund",
    "revenue",
    "sales",
    "spend",
    "value",
}

_NON_MONETARY_TERMS = {
    "count",
    "customer",
    "customers",
    "load",
    "orders",
    "pct",
    "percent",
    "percentage",
    "rate",
    "ratio",
    "share",
    "signup",
    "signups",
    "ticket",
    "tickets",
    "unit",
    "units",
}


@dataclass(frozen=True, slots=True)
class PresentationGuidance:
    """Serializable guidance derived from an analyst profile presentation prompt."""

    profile_name: str
    agent_name: str
    instructions: str
    formatting: dict[str, Any]

    @classmethod
    def from_prompt(
        cls,
        *,
        profile_name: str,
        agent_name: str,
        prompt: str | None,
    ) -> "PresentationGuidance | None":
        instructions = str(prompt or "").strip()
        if not instructions:
            return None
        return cls(
            profile_name=profile_name,
            agent_name=agent_name,
            instructions=instructions,
            formatting=_formatting_from_prompt(instructions),
        )

    def to_dict(self) -> dict[str, Any]:
        return {
            "profile_name": self.profile_name,
            "agent_name": self.agent_name,
            "instructions": self.instructions,
            "formatting": self.formatting,
        }


def build_column_formatting(
    *,
    columns: list[Any],
    presentation_guidance: dict[str, Any] | None,
) -> dict[str, Any]:
    """Build UI-friendly formatting metadata for tabular/chart artifacts."""

    if not presentation_guidance:
        return {}
    formatting = presentation_guidance.get("formatting")
    if not isinstance(formatting, dict):
        return {}
    currency = formatting.get("currency")
    if not isinstance(currency, dict):
        return {}

    number_format = formatting.get("number") if isinstance(formatting.get("number"), dict) else {}
    column_format: dict[str, Any] = {}
    for column in columns:
        column_name = _column_name(column)
        if not column_name or not _looks_monetary(column_name):
            continue
        column_format[column_name] = {
            "kind": "currency",
            "currency": currency.get("code"),
            "symbol": currency.get("symbol"),
            "use_grouping": bool(number_format.get("use_grouping", True)),
            "maximum_fraction_digits": number_format.get("maximum_fraction_digits", 2),
            "small_number_maximum_fraction_digits": number_format.get(
                "small_number_maximum_fraction_digits",
                3,
            ),
            "small_number_threshold": number_format.get("small_number_threshold", 1),
        }

    return {"columns": column_format} if column_format else {}


def _formatting_from_prompt(prompt: str) -> dict[str, Any]:
    lower = prompt.lower()
    currency = _currency_from_prompt(lower)
    no_decimals = bool(
        re.search(r"\b(no|zero|0)\s+decimal", lower)
        or "whole number" in lower
        or "whole numbers" in lower
    )
    formatting: dict[str, Any] = {}
    if currency is not None:
        formatting["currency"] = currency
    if currency is not None or "comma" in lower or "commas" in lower:
        formatting["number"] = {
            "use_grouping": "comma" in lower or "commas" in lower or currency is not None,
            "maximum_fraction_digits": 0 if no_decimals else 2,
            "small_number_maximum_fraction_digits": 3,
            "small_number_threshold": 1,
        }
    return formatting


def _currency_from_prompt(lower_prompt: str) -> dict[str, str] | None:
    for code, symbol, markers in _CURRENCY_MARKERS:
        if any(_contains_currency_marker(lower_prompt, marker) for marker in markers):
            return {"code": code, "symbol": symbol}
    return None


def _contains_currency_marker(lower_prompt: str, marker: str) -> bool:
    normalized_marker = marker.lower().strip()
    if not normalized_marker:
        return False
    if re.search(r"[a-z0-9]", normalized_marker):
        return re.search(
            rf"(?<![a-z0-9]){re.escape(normalized_marker)}(?![a-z0-9])",
            lower_prompt,
        ) is not None
    return normalized_marker in lower_prompt


def _column_name(column: Any) -> str:
    if isinstance(column, str):
        return column.strip()
    if isinstance(column, dict):
        for key in ("name", "key", "label"):
            value = column.get(key)
            if isinstance(value, str) and value.strip():
                return value.strip()
    return ""


def _looks_monetary(column_name: str) -> bool:
    normalized = re.sub(r"[^a-z0-9]+", "_", column_name.lower()).strip("_")
    if not normalized:
        return False
    terms = {term for term in normalized.split("_") if term}
    if terms & {"cac"}:
        return True
    if terms & {"cost", "spend", "revenue", "sales", "amount", "price", "value", "pipeline"}:
        return not bool(terms & {"rate", "ratio", "share", "percent", "pct"})
    if terms & {"gross", "net", "margin"}:
        return not bool(terms & _NON_MONETARY_TERMS)
    return bool(terms & _MONETARY_TERMS) and not bool(terms & _NON_MONETARY_TERMS)


__all__ = ["PresentationGuidance", "build_column_formatting"]
