import type {Run} from '../types/entities';import {runLabels} from '../constants/enums';
/*
 * The selector states the persisted status, retry count and the recent failure
 * directly, so a pending/interrupted run is distinguishable from a live one.
 */
export default function RunSelector({runs,value,onChange}:{runs:Run[];value:string;onChange:(s:string)=>void}){
 return <label>计算记录<select aria-label="计算记录" value={value} onChange={e=>onChange(e.target.value)}>{runs.map(r=>{const missing=r.status==='completed'&&r.artifact_present===false;const retried=r.last_error?` · 尝试${r.attempts}/${r.max_attempts}`:'';return <option key={r.id} value={r.id}>{r.id} · {missing?'结果文件待恢复':(runLabels[r.status]||r.status)}{retried}</option>})}</select></label>}
