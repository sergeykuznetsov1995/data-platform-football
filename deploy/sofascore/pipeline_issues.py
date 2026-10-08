"""Explicit opt-in GitHub incident publication; no notifications or source I/O."""
from __future__ import annotations

import json
import subprocess

REPO = "sergeykuznetsov1995/data-platform-football"
PROJECT = "PVT_kwHOA8FU3c4BXy0F"  # Data Platform #2


class GitHubIssues:
    @staticmethod
    def gh(*args, stdin=None):
        try:
            result = subprocess.run(["gh", *args], input=stdin, text=True,
                                    capture_output=True, timeout=60, check=False)
        except subprocess.TimeoutExpired:
            raise RuntimeError("GitHub timeout; reconcile before retry") from None
        if result.returncode:
            # Credentials or response bodies must not enter incident evidence.
            raise RuntimeError("GitHub operation failed; reconcile before retry")
        return result.stdout.strip()

    def find(self, marker):
        # REST list is authoritative and paginated, unlike the eventually
        # consistent issue search index. Check open AND closed bodies.
        pages = json.loads(self.gh("api", "--paginate", "--slurp",
                                  f"repos/{REPO}/issues?state=all&per_page=100"))
        matches = [row for page in pages for row in page
                   if "pull_request" not in row and marker in (row.get("body") or "")]
        if len(matches) > 1:
            raise RuntimeError("duplicate incident markers require reconciliation")
        return {"number": matches[0]["number"], "node_id": matches[0]["node_id"]} if matches else None

    def create(self, incident):
        lane, kind = incident["rule"].split(":")
        payload = {
            "title": f"SofaScore: {lane} / {kind} / {incident['day']} UTC",
            "body": "\n".join([
                incident["marker"], "", "Refs #1361", "",
                f"Класс: {kind}; полоса: {lane}; начало: {incident['started_at']}.",
                incident["detail"], "", "Доказательства:",
                *[f"- {path}" for path in incident["evidence"]], "",
                "Событие зафиксировано сторожем. Причина требует диагностики; автоматического исправления нет.",
            ]),
        }
        response = json.loads(self.gh("api", f"repos/{REPO}/issues", "--method", "POST",
                                      "--input", "-", stdin=json.dumps(payload, ensure_ascii=False)))
        return {"number": response["number"], "node_id": response["node_id"]}

    def add_project(self, issue):
        payload = {"query": "mutation($p:ID!,$c:ID!){addProjectV2ItemById(input:{projectId:$p,contentId:$c}){item{id}}}",
                   "variables": {"p": PROJECT, "c": issue["node_id"]}}
        response = json.loads(self.gh("api", "graphql", "--input", "-", stdin=json.dumps(payload)))
        if response.get("errors") or not response.get("data", {}).get("addProjectV2ItemById", {}).get("item", {}).get("id"):
            raise RuntimeError("Project addition unconfirmed")
