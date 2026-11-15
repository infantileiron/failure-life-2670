import {runLabels} from '../../constants/enums';
import {timestamp} from '../../utils/format';
import type {Run} from '../../types/entities';
/*
 * Completion is a promise about both the database row and the artifact on
 * disk. If the artifact is missing, the run is shown as recovering rather than
 * completed. Retry count and the most recent failure stay visible so a
 * reviewer can audit what happened before the final state.
 */
export default function StatusBadge({status,run}:{status:string;run?:Run}){
  const missing=run&&run.status==='completed'&&run.artifact_present===false;
  const label=missing?'结果文件待恢复':(runLabels[status]||status);
  return <span className={'badge '+(missing?'failed':status)}>{label}</span>;
}
export function RunLifecycle({run}:{run:Run}){
  const final=run.final_status||(run.status==='completed'?'completed':run.status==='failed'?'failed':'');
  return <section className="paper run-lifecycle">
    <h2>运行状态与恢复记录</h2>
    <dl className="lifecycle-grid">
      <div><dt>当前状态</dt><dd><StatusBadge status={run.status} run={run}/></dd></div>
      <div><dt>最终状态</dt><dd>{final==='completed'?'已完成并发布':final==='failed'?'最终失败（不再自动重试）':'进行中，尚未定稿'}</dd></div>
      <div><dt>计算尝试次数</dt><dd>{run.attempts} / {run.max_attempts}{run.retryable?' · 等待恢复重算':''}</dd></div>
      <div><dt>最近失败原因</dt><dd className={run.last_error?'failure-reason':''}>{run.last_error||'无失败记录'}</dd></div>
      <div><dt>最近失败时间</dt><dd>{timestamp(run.last_error_at)}</dd></div>
      <div><dt>执行进程</dt><dd>{run.worker||'—'}</dd></div>
      <div><dt>结果文件</dt><dd>{run.status==='completed'?(run.artifact_present===false?'缺失，等待恢复重算':(run.artifact||'已发布')):run.artifact?'历史文件保留':'-'}</dd></div>
      <div><dt>完成时间</dt><dd>{timestamp(run.finished_at||run.published_at)}</dd></div>
    </dl>
    {run.artifact_sha256&&<small className="break">结果文件 SHA256 {run.artifact_sha256}</small>}
  </section>;
}
