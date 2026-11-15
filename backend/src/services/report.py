import csv, io, os, tempfile
from fastapi import HTTPException
from src.models.entities import Report
from src.config.settings import ARTIFACT_DIR
from src.utils import repository as repo
from src.utils.identity import new_id, now
from src.services.run import artifact_missing


def safe_cell(value):
    return "'" + value if isinstance(value, str) and value.lstrip().startswith(('=', '+', '-', '@')) else value


def list_reports():
    return repo.all_rows('reports')


def _report_path(report_id):
    return ARTIFACT_DIR / (report_id + '.csv')


def _write_report_file(report_id, content):
    """Atomically write the CSV and only then make the report visible.

    Writing to a staging file first means a crash or a full disk cannot leave a
    report row pointing at a truncated or missing file.
    """
    ARTIFACT_DIR.mkdir(parents=True, exist_ok=True)
    fd, tmp = tempfile.mkstemp(dir=str(ARTIFACT_DIR), prefix='.' + report_id + '.', suffix='.tmp')
    try:
        with os.fdopen(fd, 'w', encoding='utf-8') as handle:
            handle.write(content)
            handle.flush()
            os.fsync(handle.fileno())
        os.replace(tmp, _report_path(report_id))
    except BaseException:
        try: os.unlink(tmp)
        except OSError: pass
        raise


def create(data):
    run, review = repo.get('runs', data.run_id), repo.get('reviews', data.review_id)
    if review['status'] != 'approved' or review['run_id'] != run['id'] or run['status'] != 'completed':
        raise ValueError('报告必须引用该运行已通过的复核')
    if artifact_missing(run):
        raise ValueError('结果文件缺失，页面不会标记完成；请等待自动恢复重算后再生成报告')
    report_id = new_id('rpt')
    # Render and persist the CSV before inserting the report row, so a
    # successful response always corresponds to a downloadable file.
    _write_report_file(report_id, render(report_id, data.title, run))
    report = Report(report_id, data.run_id, data.review_id, data.title, now())
    return repo.put('reports', report)


def render(report_id, title, run):
    r = run['result']
    lr = r['logrank']
    out = io.StringIO()
    w = csv.writer(out)
    w.writerow(['report', 'run_id', 'included', 'excluded_other_groups', 'administratively_censored',
                'chi_square_df1', 'p_value', 'logrank_estimable', 'checksum'])
    w.writerow([safe_cell(title), run['id'], r['included'], r['excluded_other_groups'],
                r['administratively_censored'], lr['chi_square'], lr['p_value'], lr['estimable'],
                r['checksum']])
    w.writerow([])
    w.writerow(['group', 'time_hours', 'at_risk', 'events', 'censored', 'risk_after', 'survival',
                'greenwood_variance', 'ci95_low', 'ci95_high', 'median_hours'])
    for g in r['groups']:
        for p in g['curve']:
            w.writerow([safe_cell(g['group']), p['time'], p['at_risk'], p['events'], p['censored'],
                        p['risk_after'], p['survival'], p['variance'], p['ci95_low'],
                        p['ci95_high'], g['median_hours']])
    w.writerow([])
    w.writerow(['event_time', 'n_a', 'n_b', 'd_a', 'd_b', 'expected_a',
                'observed_minus_expected_a', 'variance_contribution'])
    for p in lr['contributions']:
        w.writerow([p[k] for k in ['time', 'n_a', 'n_b', 'd_a', 'd_b', 'expected_a',
                                   'observed_minus_expected_a', 'variance_contribution']])
    return '﻿' + out.getvalue()


def download(id):
    report = repo.get('reports', id)
    run = repo.get('runs', report['run_id'])
    path = _report_path(id)
    if not path.is_file():
        raise HTTPException(404, '报告文件缺失，请重新生成')
    if repo.get('reviews', report['review_id'])['status'] != 'approved':
        raise HTTPException(403, '复核未通过')
    return path.read_text(encoding='utf-8')
