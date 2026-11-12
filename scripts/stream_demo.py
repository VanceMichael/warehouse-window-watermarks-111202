import json
import subprocess
import sys
import tempfile
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
STREAM = ROOT / 'examples/stream'


def execute(*arguments, expect_error=False):
    command = [sys.executable, '-m', 'warehouse', *arguments]
    print('$ ' + ' '.join(command), flush=True)
    result = subprocess.run(command, cwd=ROOT, text=True, capture_output=True)
    if expect_error:
        if result.returncode == 0:
            raise RuntimeError('预期失败的命令却成功了')
        print(result.stderr.strip(), flush=True)
        return result.stderr.strip()
    if result.returncode != 0:
        print(result.stdout, flush=True)
        print(result.stderr, flush=True)
        raise RuntimeError('命令执行失败')
    print(result.stdout.strip(), flush=True)
    return json.loads(result.stdout)


def require(condition, message):
    if not condition:
        raise RuntimeError(message)


def main():
    with tempfile.TemporaryDirectory() as temporary:
        database = str(Path(temporary) / 'warehouse.sqlite3')
        staged_path = Path(temporary) / 'staged-05.json'

        print('== 1. 首微批：分区内乱序按事件时间归属 ==', flush=True)
        summary = execute('batch', '--db', database, '--input', str(STREAM / 'batch-01.json'), '--tick', '1')
        require(summary['days'] == [{'day': '2026-10-01', 'units': 30, 'version': 1}], '首日合计应为 30')

        print('== 2. d2 停发：全局水位取有效分区最小值，被 d2 拖住 ==', flush=True)
        summary = execute('batch', '--db', database, '--input', str(STREAM / 'batch-02.json'), '--tick', '2')
        require(summary['watermark'] == '2026-10-01T10:30:00+00:00', '全局水位应为 min(d1, d2)')

        print('== 3. d2 持续静默：按处理时钟转空闲，水位推进、窗口封口 ==', flush=True)
        summary = execute('batch', '--db', database, '--input', str(STREAM / 'batch-03.json'), '--tick', '3')
        require(summary['idle'] == ['d2'], 'd2 应转为空闲')
        require(summary['sealed'] == ['2026-10-01T10:00:00+00:00', '2026-10-01T11:00:00+00:00'], '10 点与 11 点窗口应封口')
        state = execute('watermarks', '--db', database)
        require(state['watermark'] == '2026-10-01T12:00:00+00:00', '空闲分区不再拖累水位')

        print('== 4. d2 恢复补发：范围内修订封口窗口，超范围进入回补队列 ==', flush=True)
        summary = execute('batch', '--db', database, '--input', str(STREAM / 'batch-04.json'), '--tick', '4')
        require(summary['reactivated'] == ['d2'] and summary['revised'] == 1 and summary['queued'] == 1,
                '补发应分别进入修订与回补队列')
        require(summary['watermark'] == '2026-10-01T12:00:00+00:00', '重新活跃不能令水位倒退')
        require(summary['days'][0]['units'] == 163, '迟到修订后日值应为 163')
        late = execute('late', '--db', database)
        require([row['event'] for row in late['queued']] == ['e-d2-004'], '超范围迟到应排队等候回补')
        require([row['event'] for row in late['revised']] == ['e-d2-003'], '范围内迟到应已修订窗口')

        print('== 5. 故障重开：重放同一微批不丢窗口、不重复贡献 ==', flush=True)
        summary = execute('batch', '--db', database, '--input', str(STREAM / 'batch-04.json'), '--tick', '4')
        require(summary['changed'] is False and summary['duplicates'] == 3, '重放不应产生新贡献')
        require(execute('daily', '--db', database)[0]['units'] == 163, '重放后日值不变')

        print('== 6. 同位置冲突：停止并诊断，整批不落库 ==', flush=True)
        execute('batch', '--db', database, '--input', str(STREAM / 'batch-conflict.json'), '--tick', '4', expect_error=True)
        require(execute('daily', '--db', database)[0]['units'] == 163, '冲突后日值应保持不变')

        print('== 7. 离线回补与实时竞争：旧微批不得覆盖校正版 ==', flush=True)
        staged = execute('prepare', '--db', database, '--input', str(STREAM / 'batch-05.json'), '--tick', '5')
        staged_path.write_text(json.dumps(staged, ensure_ascii=False), encoding='utf-8')
        correction = execute('backfill', '--db', database, '--tick', '5')
        require(correction['resolved'] == 1 and correction['days'][0]['units'] == 223, '回补应替换封口窗口并推进日值')
        execute('commit', '--db', database, '--staged', str(staged_path), expect_error=True)
        require(execute('daily', '--db', database)[0]['units'] == 223, '旧微批不得覆盖校正版')
        summary = execute('batch', '--db', database, '--input', str(STREAM / 'batch-05.json'), '--tick', '5')
        require(summary['days'][0]['units'] == 227, '重新准备后微批应叠在校正版之上')
        explain = execute('explain', '--db', database, '--day', '2026-10-01')
        require('backfill:1' in [row['cause'] for row in explain['history']], '历史应记录回补造成的变化')
        corrected = [w for w in explain['windows'] if w['corrected']]
        require(corrected and corrected[0]['window'] == '2026-10-01T08:00:00+00:00', '回补窗口应可解释')

        print('== 8. 跨日窗口：水位跨过午夜才封存首日 ==', flush=True)
        summary = execute('batch', '--db', database, '--input', str(STREAM / 'batch-06.json'), '--tick', '6')
        require(summary['days'] == [{'day': '2026-10-01', 'units': 233, 'version': 7},
                                    {'day': '2026-10-02', 'units': 4, 'version': 1}], '跨日事件应各归其日')
        summary = execute('batch', '--db', database, '--input', str(STREAM / 'batch-07.json'), '--tick', '7')
        require(summary['sealed'] == [] and summary['watermark'] == '2026-10-01T23:55:00+00:00',
                'd2 水位停在子夜前，23 点窗口不得提前封口')
        print('== 8b. 空微批：d2 静默转空闲，水位跨过午夜封存首日 ==', flush=True)
        summary = execute('batch', '--db', database, '--input', str(STREAM / 'batch-08.json'), '--tick', '8')
        require(summary['idle'] == ['d2'], 'd2 应再次转为空闲')
        require('2026-10-01T23:00:00+00:00' in summary['sealed'], '23 点窗口应在水位跨日后封口')
        explain = execute('explain', '--db', database, '--day', '2026-10-01')
        require(all(window['sealed'] for window in explain['windows']), '首日窗口应全部封口')

        print('== 9. 最终对齐：微批日值 == 事件时间完整重算 ==', flush=True)
        result = execute('verify', '--db', database)
        require(result['aligned'] and result['final'], '回补后日值应与完整重算对齐')
        execute('import', '--db', database, '--input', str(STREAM / 'full-readings.json'), '--batch', 'full')
        calculated = execute('inspect', '--db', database)
        mart = execute('daily', '--db', database)
        require([{'day': row['day'], 'units': row['units']} for row in mart] == calculated,
                '日层集市应与现有全量重算一致')
        print('微批日值与事件时间完整重算一致，全程时钟由入口传入', flush=True)


if __name__ == '__main__':
    main()
