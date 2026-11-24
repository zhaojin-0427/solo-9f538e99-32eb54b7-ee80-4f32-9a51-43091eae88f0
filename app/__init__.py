"""Flask 核账页应用工厂。"""
from __future__ import annotations

import os

from flask import (Flask, Response, flash, redirect, render_template,
                   request, url_for)

from . import service, storage


def create_app(db_path: str | None = None) -> Flask:
    app = Flask(__name__)
    app.secret_key = os.environ.get("SECRET_KEY", "dev-secret-key")
    db_path = db_path or os.environ.get(
        "DB_PATH", os.path.join(os.getcwd(), "data", "meter.db"))
    os.makedirs(os.path.dirname(os.path.abspath(db_path)), exist_ok=True)
    app.config["DB_PATH"] = db_path

    def conn():
        return storage.get_db(app.config["DB_PATH"])

    @app.get("/")
    def index():
        view = service.dashboard(conn())
        return render_template("index.html", **view)

    @app.post("/upload")
    def upload():
        file = request.files.get("csv")
        if not file or not file.filename:
            flash("请选择 CSV 文件", "error")
            return redirect(url_for("index"))
        try:
            incoming = storage.parse_csv(file.read())
            outcome = service.submit_readings(conn(), incoming)
        except storage.UploadError as exc:
            flash(f"上传被整批拒绝：{exc}（库内读数与结论不变）", "error")
            return redirect(url_for("index"))
        except storage.ConflictError as exc:
            flash(f"提交冲突，本次补传未生效：{exc}", "error")
            return redirect(url_for("index"))
        msg = (f"上传成功：合并后共 {outcome.total} 条读数"
               f"（同刻覆盖 {outcome.overwritten} 条，"
               f"新增 {outcome.added} 条）")
        if outcome.recomputed:
            labels = {"feasible": "核对通过",
                      "target_unreachable": "所报时段达不到峰值",
                      "others_breach": "可达但其余时段守不住上限"}
            msg += ("；已按现行参数重算并更新当前结论："
                    f"{labels[outcome.result.status]}")
        else:
            msg += "；现行参数未齐备，未触发重算"
        flash(msg, "ok")
        return redirect(url_for("index"))

    @app.post("/audit")
    def audit():
        try:
            result = service.run_audit(
                conn(),
                request.form.get("per_minute_cap", ""),
                request.form.get("peak_wh", ""),
                request.form.get("reported_window", ""))
        except service.AuditInputError as exc:
            flash(f"未执行核对：{exc}（已有结论保持不变）", "error")
            return redirect(url_for("index"))
        except storage.ConflictError as exc:
            flash(f"提交冲突，本次核对未生效：{exc}", "error")
            return redirect(url_for("index"))
        labels = {"feasible": "核对通过",
                  "target_unreachable": "所报时段达不到峰值",
                  "others_breach": "可达但其余时段守不住上限"}
        flash(f"核对完成：{labels[result.status]} —— {result.message}",
              "ok" if result.status == "feasible" else "error")
        return redirect(url_for("index"))

    @app.get("/windows.csv")
    def windows_csv():
        rows = service.windows_csv_rows(conn())
        if not rows:
            flash("暂无可导出的各时段电量", "error")
            return redirect(url_for("index"))
        body = ["window_start,window_index,energy_wh,is_reported"]
        body += [f"{ts},{i},{wh},{int(flag)}" for ts, wh, i, flag in rows]
        return Response("\n".join(body) + "\n", mimetype="text/csv",
                        headers={"Content-Disposition":
                                 "attachment; filename=windows.csv"})

    @app.post("/reset")
    def reset():
        storage.reset_all(conn())
        flash("已清空读数、参数与全部结论（账期代号已更新，"
              "在途提交不会回写新账期）", "ok")
        return redirect(url_for("index"))

    return app
