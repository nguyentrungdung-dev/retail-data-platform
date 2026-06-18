"""
Auto-provision Metabase dashboards từ YAML config.

Usage:
    # Dry-run: chỉ parse YAML, KHÔNG gọi Metabase API
    python provision.py --dry-run

    # Provision thật
    python provision.py

    # Idempotent: nếu dashboard cùng tên đã tồn tại → archive cái cũ rồi tạo mới
    python provision.py --replace

Yêu cầu env vars (lấy từ .env qua dotenv):
    MB_URL          mặc định http://localhost:3000
    MB_USER         email admin Metabase
    MB_PASSWORD     password admin
    MB_DATABASE     tên database trong Metabase, mặc định 'retail_dw'
"""
from __future__ import annotations

import argparse
import os
import sys
from pathlib import Path
from typing import Any

import requests
import yaml
from dotenv import load_dotenv

load_dotenv(override=False)

MB_URL      = os.getenv("MB_URL", "http://localhost:3000").rstrip("/")
MB_USER     = os.getenv("MB_USER", "")
MB_PASSWORD = os.getenv("MB_PASSWORD", "")
MB_DATABASE = os.getenv("MB_DATABASE", "retail_dw")

QUERIES_DIR = Path(__file__).parent / "queries"


# ── METABASE API CLIENT ─────────────────────────────────────────

class MetabaseClient:
    def __init__(self, base_url: str, username: str, password: str) -> None:
        self.base_url = base_url
        self.session = requests.Session()
        self._login(username, password)

    def _login(self, username: str, password: str) -> None:
        resp = self.session.post(
            f"{self.base_url}/api/session",
            json={"username": username, "password": password},
            timeout=15,
        )
        if resp.status_code != 200:
            raise RuntimeError(
                f"Login Metabase thất bại ({resp.status_code}): {resp.text[:300]}\n"
                f"→ Kiểm tra MB_USER/MB_PASSWORD trong .env, hoặc Metabase chưa "
                f"setup admin (mở {self.base_url} để hoàn tất setup wizard)."
            )
        token = resp.json()["id"]
        self.session.headers.update({"X-Metabase-Session": token})

    def _req(self, method: str, path: str, **kwargs: Any) -> Any:
        resp = self.session.request(method, f"{self.base_url}{path}", timeout=30, **kwargs)
        if not resp.ok:
            raise RuntimeError(f"{method} {path} → {resp.status_code}: {resp.text[:300]}")
        return resp.json() if resp.content else {}

    def get_database_id(self, name: str) -> int:
        result = self._req("GET", "/api/database")
        databases = result.get("data", result) if isinstance(result, dict) else result
        for db in databases:
            if db.get("name", "").lower() == name.lower():
                return int(db["id"])
        # fallback: chứa substring
        for db in databases:
            if name.lower() in db.get("name", "").lower():
                return int(db["id"])
        names = [db.get("name") for db in databases]
        raise RuntimeError(
            f"Không tìm thấy database '{name}' trong Metabase. "
            f"Databases hiện có: {names}\n"
            f"→ Vào {self.base_url}/admin/databases để add Postgres "
            f"(host=postgres-dwh, port=5432, db=retail_dw, user=retail)."
        )

    def find_collection_id(self, name: str) -> int | None:
        for col in self._req("GET", "/api/collection"):
            if col.get("name") == name:
                return col.get("id")
        return None

    def ensure_collection(self, name: str) -> int:
        cid = self.find_collection_id(name)
        if cid is not None:
            return cid
        result = self._req("POST", "/api/collection", json={"name": name, "color": "#509EE3"})
        return int(result["id"])

    def archive_dashboard_if_exists(self, name: str) -> None:
        for d in self._req("GET", "/api/dashboard"):
            if d.get("name") == name and not d.get("archived"):
                self._req("PUT", f"/api/dashboard/{d['id']}", json={"archived": True})

    def create_card(self, db_id: int, card_def: dict, collection_id: int) -> int:
        viz = card_def.get("visualization", "table")
        payload = {
            "name": card_def["name"],
            "dataset_query": {
                "type": "native",
                "native": {"query": card_def["sql"].strip()},
                "database": db_id,
            },
            "display": viz,
            "visualization_settings": card_def.get("viz_settings", {}),
            "collection_id": collection_id,
        }
        result = self._req("POST", "/api/card", json=payload)
        return int(result["id"])

    def create_dashboard(
        self,
        name: str,
        description: str,
        cards: list[tuple[int, dict]],
        collection_id: int,
    ) -> int:
        result = self._req(
            "POST", "/api/dashboard",
            json={"name": name, "description": description, "collection_id": collection_id},
        )
        dash_id = int(result["id"])

        # Add cards với layout. Metabase 0.50+ dùng PUT /api/dashboard/:id
        # với "dashcards" array; id âm = card mới.
        dashcards = []
        for idx, (card_id, card_def) in enumerate(cards):
            pos = card_def.get("position", {})
            dashcards.append({
                "id":         -(idx + 1),
                "card_id":    card_id,
                "row":        pos.get("row", idx * 4),
                "col":        pos.get("col", 0),
                "size_x":     pos.get("size_x", 12),
                "size_y":     pos.get("size_y", 4),
                "parameter_mappings":     [],
                "visualization_settings": card_def.get("viz_settings", {}),
            })
        self._req("PUT", f"/api/dashboard/{dash_id}", json={"dashcards": dashcards})
        return dash_id


# ── MAIN FLOW ───────────────────────────────────────────────────

def load_dashboard_specs() -> list[dict]:
    """Đọc tất cả YAML trong queries/ theo thứ tự alphabet (01_, 02_, ...)."""
    specs = []
    for path in sorted(QUERIES_DIR.glob("*.yml")):
        with path.open(encoding="utf-8") as f:
            spec = yaml.safe_load(f)
        spec["_source"] = path.name
        specs.append(spec)
    return specs


def validate_spec(spec: dict) -> list[str]:
    """Validate cấu trúc YAML, trả về list lỗi (rỗng = OK)."""
    errors: list[str] = []
    src = spec.get("_source", "?")
    if not spec.get("name"):
        errors.append(f"{src}: thiếu 'name'")
    cards = spec.get("cards", [])
    if not cards:
        errors.append(f"{src}: không có card nào")
    for i, card in enumerate(cards):
        prefix = f"{src} card[{i}]"
        if not card.get("name"):
            errors.append(f"{prefix}: thiếu 'name'")
        if not card.get("sql"):
            errors.append(f"{prefix}: thiếu 'sql'")
        viz = card.get("visualization")
        if viz and viz not in {"scalar", "line", "bar", "row", "table", "pie", "area"}:
            errors.append(f"{prefix}: visualization '{viz}' không hỗ trợ")
    return errors


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--dry-run", action="store_true",
                        help="Chỉ parse YAML, không gọi API")
    parser.add_argument("--replace", action="store_true",
                        help="Archive dashboard cũ trùng tên trước khi tạo mới")
    args = parser.parse_args()

    print(f"📂 Đọc dashboard specs từ {QUERIES_DIR}")
    specs = load_dashboard_specs()
    print(f"   Tìm thấy {len(specs)} dashboard\n")

    all_errors: list[str] = []
    for spec in specs:
        errors = validate_spec(spec)
        if errors:
            all_errors.extend(errors)
        else:
            n_cards = len(spec.get("cards", []))
            print(f"   ✓ {spec['_source']}: {spec['name']} ({n_cards} cards)")
    if all_errors:
        print("\n❌ Validation FAIL:")
        for e in all_errors:
            print(f"   - {e}")
        return 1
    print()

    if args.dry_run:
        print("✅ Dry-run OK. Chạy lại không có --dry-run để provision thật.")
        return 0

    if not MB_USER or not MB_PASSWORD:
        print("❌ Thiếu MB_USER / MB_PASSWORD trong .env")
        return 1

    print(f"🔐 Đang đăng nhập Metabase tại {MB_URL} ...")
    client = MetabaseClient(MB_URL, MB_USER, MB_PASSWORD)
    print(f"   OK\n")

    db_id = client.get_database_id(MB_DATABASE)
    print(f"🗄  Database '{MB_DATABASE}' → id={db_id}\n")

    collection_id = client.ensure_collection("Retail Dashboards")
    print(f"📁 Collection 'Retail Dashboards' → id={collection_id}\n")

    for spec in specs:
        name = spec["name"]
        print(f"🛠  Provisioning '{name}' ...")

        if args.replace:
            client.archive_dashboard_if_exists(name)

        # Tạo từng card trước
        cards_created: list[tuple[int, dict]] = []
        for card in spec["cards"]:
            cid = client.create_card(db_id, card, collection_id)
            cards_created.append((cid, card))
            print(f"      + card '{card['name']}' (id={cid})")

        # Sau đó tạo dashboard và gắn cards
        dash_id = client.create_dashboard(
            name=name,
            description=spec.get("description", ""),
            cards=cards_created,
            collection_id=collection_id,
        )
        print(f"   ✅ Dashboard id={dash_id} → {MB_URL}/dashboard/{dash_id}\n")

    print("🎉 Hoàn tất! Vào Metabase UI để xem.")
    return 0


if __name__ == "__main__":
    sys.exit(main())
